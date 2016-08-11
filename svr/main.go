package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/standard"
	"github.com/leebenson/conform"
	"github.com/sharonjl/go-commons/math"
	"github.com/sharonjl/go-commons/queue"
	"github.com/twinj/uuid"
	"gopkg.in/bluesuncorp/validator.v9"
	"gopkg.in/guregu/null.v3"
	"log"
	"net/http"
	"strings"
	"time"
)

var myDB Database
var svr *echo.Echo
var facilities []string
var valid = validator.New()
var q queue.Queue
var pushSvc PushService

const (
	HeaderFacility string = "X-Facility"
)

var (
	fPort       string = "8080"
	fAWSKey     string = ""
	fAWSSecret  string = ""
	fDBName     string = ""
	fDBHost     string = ""
	fDBUser     string = ""
	fDBPort     string = ""
	fDBPassword string = ""
	fFacilities string = ""
	fSQSQueue   string = ""
	fSQSRegion  string = ""
	fOSAppID    string = ""
)

func init() {
	valid.RegisterValidation("pushprovider", func(fl validator.FieldLevel) bool {
		switch fl.Field().String() {
		case "apns":
			return true
		}
		return false
	})

	flag.StringVar(&fPort, "p", "", "HTTP port")
	flag.StringVar(&fFacilities, "f", "", "Allowed facility secrets")
	flag.StringVar(&fAWSKey, "aws:key", "", "AWS access key")
	flag.StringVar(&fAWSSecret, "aws:secret", "", "AWS secret key")
	flag.StringVar(&fDBName, "db:name", "", "Database name")
	flag.StringVar(&fDBHost, "db:host", "", "Database host")
	flag.StringVar(&fDBUser, "db:user", "", "Database user")
	flag.StringVar(&fDBPort, "db:port", "", "Database port")
	flag.StringVar(&fDBPassword, "db:password", "", "Database password")
	flag.StringVar(&fSQSQueue, "sqs:q", "", "SQS queue name")
	flag.StringVar(&fSQSRegion, "sqs:region", "", "SQS Region")
	flag.StringVar(&fOSAppID, "push:os:app_id", "", "One signal app id")
}
func main() {

	flag.Parse()
	facilities = strings.Split(fFacilities, ",")

	initDB()
	initPushService()
	initHTTP()
	initQueue()

	svr.Run(standard.New(":" + fPort))
}

func initDB() {
	cstr := fmt.Sprintf("user=%s password=%s host=%s port=%s dbname=%s sslmode=disable",
		fDBUser,
		fDBPassword,
		fDBHost,
		fDBPort,
		fDBName,
	)
	db, err := sqlx.Connect("postgres", cstr)
	if err != nil {
		log.Fatalln(err)
	}
	myDB = NewPQDatabase(db)
}

func initPushService() {
	pushSvc = NewPushService(fOSAppID)
}

func initHTTP() {
	svr = echo.New()
	svr.Use(authFacility)

	svr.POST("/devices", addDevice)
	svr.DELETE("/devices/:id", removeDevice)
	svr.DELETE("/devices/token/:token", removeDeviceToken)
	svr.DELETE("/devices/user/:id", removeUserDevices)

	svr.POST("/interests", addInterest)
	svr.DELETE("/interests/user/:user", removeUserInterests)
	svr.DELETE("/interests/user/:user/verb/:verb/object/:object", removeTopicForUser)

	svr.POST("/push", addPushJobs)
}

func initQueue() {
	awsCreds := credentials.NewStaticCredentials(fAWSKey, fAWSSecret, "")

	config := &aws.Config{
		Region:      aws.String(fSQSRegion),
		LogLevel:    aws.LogLevel(aws.LogOff),
		Credentials: awsCreds,
	}

	q = queue.NewSQSQueue(fSQSQueue, session.New(config))
	go q.Poll(queue.SQSHandlerFunc(dispatchPushJob))
}

type pushJob struct {
	Facility    string      `json:"facility" validate:"required"`
	Topic       string      `json:"topic" validate:"required"`
	Message     string      `json:"message" validate:"required"`
	CreatedOn   time.Time   `json:"created_on" validate:"required"`
	NotifyCount int         `json:"notify_count" validate:"-"`
	Data        interface{} `json:"data" validate:"-"`
	Limit       uint        `json:"limit" validate:"-"`
	Offset      uint        `json:"offset" validate:"-"`
}

const (
	QueueBatchSize            uint = 10
	PushNotificationBatchSize uint = 1000
)

func dispatchPushJob(msg *sqs.Message) error {
	var job *pushJob

	err := json.Unmarshal([]byte(aws.StringValue(msg.Body)), &job)
	if err != nil {
		return err
	}

	// Validate job data
	if err := valid.Struct(job); err != nil {
		return err
	}

	// When limit is 0, it is considered a new job. We need to
	// determine the limit count before processing the job,
	// or splitting it up into multiple jobs.
	if job.Limit == 0 {
		job.Limit = myDB.GetInterestedUserCount(job.Facility, job.Topic)
	}

	if job.Limit > PushNotificationBatchSize { // divide push job
		roundedLimit := math.UintRoundTo(job.Limit, PushNotificationBatchSize)
		batchSize := roundedLimit / PushNotificationBatchSize
		if batchSize > QueueBatchSize {
			batchSize = QueueBatchSize
		}
		limit := roundedLimit / batchSize

		items := make([]string, batchSize)
		for k := uint(0); k < batchSize; k++ {
			b, err := json.Marshal(&pushJob{
				Facility:    job.Facility,
				Message:     job.Message,
				Topic:       job.Topic,
				Data:        job.Data,
				NotifyCount: job.NotifyCount,
				CreatedOn:   job.CreatedOn,
				Offset:      job.Offset + (k * limit),
				Limit:       uint(limit),
			})
			if err != nil {
				return err
			}

			items[k] = string(b)
		}
		return q.Publish(items...)
	} else if job.Limit > 0 {
		// Send push
		var interests []Interest
		err := myDB.GetInterestedUsers(
			job.Facility,
			job.Topic,
			interests,
			job.Offset,
			job.Limit)
		if err != nil {
			return err
		}

		ids := make([]string, len(interests))
		for k, intr := range interests {
			ids[k] = intr.UserID
		}

		var devices []Device
		err = myDB.GetDevices(job.Facility, ids, devices)
		if err != nil {
			return err
		}

		apnsTokens := make([]string, len(devices))
		apnsTokenCnt := 0
		for _, dev := range devices {
			if dev.Provider == "apns" {
				apnsTokens[apnsTokenCnt] = dev.Token
				apnsTokenCnt += 1
			}
		}

		// Send push
		err = pushSvc.Push(job.Message, job.NotifyCount, job.Data, apnsTokens[:apnsTokenCnt])
		if err != nil {
			return err
		}
	}

	return nil
}

// authFacility middleware restricts requests made to white listed
// facility only. The middleware reads the x-facility header, a
// 401 Unauthorized HTTP status is issued if the facility is not
// recognized.
func authFacility(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		h := c.Request().Header()
		if h.Contains(HeaderFacility) {
			f := strings.TrimSpace(h.Get(HeaderFacility))
			if f != "" {
				for _, fac := range facilities {
					if fac == f {
						c.Set(HeaderFacility, fac)
						return next(c)
					}
				}
			}
		}
		return c.NoContent(http.StatusUnauthorized)
	}
}

type addDeviceForm struct {
	UserID         string `json:"user_id" conform:"trim" validate:"required"`
	DeviceToken    string `json:"device_token" conform:"trim" validate:"required"`
	DeviceProvider string `json:"device_provider" conform:"trim" validate:"required,pushprovider"`
}

func addDevice(c echo.Context) error {
	var frm addDeviceForm
	if err := c.Bind(&frm); err != nil {
		return err
	}

	conform.Strings(&frm)
	if err := valid.Struct(&frm); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	fac := c.Get(HeaderFacility).(string)
	device := &Device{
		Facility: fac,
		UserID:   frm.UserID,
		Token:    frm.DeviceToken,
		Provider: frm.DeviceProvider,
	}
	if err := myDB.AddDevice(device); err != nil {
		if err != ErrDeviceExists {
			log.Printf("addDevice: %s", err)
			return c.NoContent(http.StatusBadRequest)
		}
	}
	return c.NoContent(http.StatusNoContent)
}

func removeDevice(c echo.Context) error {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := myDB.RemoveDevice(&id); err != nil {
		log.Printf("removeDevice: %s", err)
		return c.NoContent(http.StatusBadRequest)
	}
	return c.NoContent(http.StatusOK)
}

func removeDeviceToken(c echo.Context) error {
	tk := strings.TrimSpace(c.Param("token"))
	if tk == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	fac := c.Get(HeaderFacility).(string)
	if err := myDB.RemoveDeviceToken(fac, tk); err != nil {
		log.Printf("removeDeviceToken: %s", err)
		return c.NoContent(http.StatusBadRequest)
	}
	return c.NoContent(http.StatusOK)
}

func removeUserDevices(c echo.Context) error {
	userID := strings.TrimSpace(c.Param("id"))
	if userID == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	fac := c.Get(HeaderFacility).(string)
	if err := myDB.RemoveUserDevices(fac, userID); err != nil {
		log.Printf("removeUserDevices: %s", err)
		return c.NoContent(http.StatusBadRequest)
	}
	return c.NoContent(http.StatusOK)
}

type addInterestForm struct {
	UserID    string `json:"user_id" conform:"trim" validate:"required"`
	Topic     string `json:"topic" conform:"trim" validate:"required"`
	ExpiresOn int64  `json:"expires_on" validate:"required"`
}

func addInterest(c echo.Context) error {
	var its []*addInterestForm
	if err := c.Bind(&its); err != nil {
		return err
	}

	k := 0
	vits := make([]*Interest, len(its))

	fac := c.Get(HeaderFacility).(string)
	for _, it := range its {
		conform.Strings(it)
		exp := null.TimeFromPtr(nil)
		if it.ExpiresOn > 0 {
			exp.SetValid(time.Unix(it.ExpiresOn, 0))
		}

		if it.UserID != "" && it.Topic != "" {
			vits[k] = &Interest{
				Facility:  fac,
				UserID:    it.UserID,
				Topic:     it.Topic,
				ExpiresOn: exp,
			}
			k += 1
		}
	}

	for _, it := range vits[:k] {
		if err := myDB.AddInterest(it); err != nil {
			if err != ErrInterestExists {
				log.Printf("addInterest: %+v\n\t%s", it, err)
			}
		}
	}
	return c.NoContent(http.StatusNoContent)
}

func removeTopicForUser(c echo.Context) error {
	user := strings.TrimSpace(c.Param("user"))
	topic := strings.TrimSpace(c.Param("topic"))
	if user == "" || topic == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	fac := c.Get(HeaderFacility).(string)
	if err := myDB.RemoveTopicForUser(fac, user, topic); err != nil {
		log.Printf("removeTopicForUser: %s", err)
		return c.NoContent(http.StatusBadRequest)
	}
	return c.NoContent(http.StatusOK)
}

func removeUserInterests(c echo.Context) error {
	user := strings.TrimSpace(c.Param("user"))
	if user == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	fac := c.Get(HeaderFacility).(string)
	if err := myDB.RemoveUserInterests(fac, user); err != nil {
		log.Printf("removeUserInterests: %s", err)
		return c.NoContent(http.StatusBadRequest)
	}
	return c.NoContent(http.StatusOK)
}

type pushForm struct {
	Topic       string      `json:"topic" conform:"trim" validate:"required"`
	Message     string      `json:"message" conform:"trim" validate:"required"`
	NotifyCount int         `json:"notify_count" validate:"required"`
	Data        interface{} `json:"data" validate:"-"`
}

func addPushJobs(c echo.Context) error {
	var forms []*pushForm
	if err := c.Bind(&forms); err != nil {
		return err
	}
	fac := c.Get(HeaderFacility).(string)

	m := make([]string, len(forms))
	for k, frm := range forms {
		conform.Strings(&frm)
		if err := valid.Struct(frm); err != nil {
			return c.NoContent(http.StatusBadRequest)
		}

		b, err := json.Marshal(&pushJob{
			Facility:    fac,
			Topic:       frm.Topic,
			Message:     frm.Message,
			CreatedOn:   time.Now(),
			NotifyCount: frm.NotifyCount,
			Data:        frm.Data,
			Limit:       0,
			Offset:      0,
		})
		if err != nil {
			return c.NoContent(http.StatusBadRequest)
		}

		m[k] = string(b)
	}

	err := q.Publish(m...)
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}
	return c.NoContent(http.StatusNoContent)
}
