package main

import (
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/twinj/uuid"
	"gopkg.in/guregu/null.v3"
	"time"
)

const pqErrUniqueConstraintViolation pq.ErrorCode = "23505"

var ErrDeviceExists = errors.New("device exists")
var ErrInterestExists = errors.New("interest exists")

type Device struct {
	ID             *uuid.Uuid `db:"id"`
	Facility       string     `db:"facility"`
	UserID         string     `db:"user_id"`
	Token          string     `db:"device_token"`
	Provider       string     `db:"device_provider"`
	LastNotifiedOn null.Time  `db:"last_notified_on"`
	CreatedOn      time.Time  `db:"created_on"`
}

// DeviceUUID generates a v5 UUID based on an resource URL that uniquely
// identifies the device
func DeviceUUID(d *Device) *uuid.Uuid {
	url := fmt.Sprintf("2p://devices/facility/%s/user/%s/provider/%s/token/%s",
		d.Facility,
		d.UserID,
		d.Provider,
		d.Token)
	id := uuid.NewV5(uuid.NameSpaceURL, uuid.Name(url))
	return &id
}

type Interest struct {
	Facility  string     `db:"facility"`
	UserID    string     `db:"user_id"`
	TopicUID  *uuid.Uuid `db:"topic_uid"`
	Topic     string     `db:"topic"`
	ExpiresOn null.Time  `db:"expires"`
	CreatedOn time.Time  `db:"created_on"`
}

// TopicUUID generates a v5 UUID based on a resource URL that uniquely
// identifies the topic.
func TopicUUID(t string) *uuid.Uuid {
	url := uuid.Name(fmt.Sprintf("2p://interest/topic/%s", t))
	id := uuid.NewV5(uuid.NameSpaceURL, url)
	return &id
}

type Database interface {
	AddDevice(device *Device) error
	RemoveDevice(id *uuid.Uuid) error
	RemoveDeviceToken(facility string, deviceID string) error
	RemoveUserDevices(facility string, userID string) error

	AddInterest(i *Interest) error
	RemoveTopic(facility string, topic string) error
	RemoveTopicForUser(
		facility,
		user string,
		topic string) error
	RemoveUserInterests(facility, user string) error

	GetInterestedUsers(
		facility,
		topic string,
		intrs []Interest,
		offset,
		limit uint) error
	GetInterestedUserCount(facility, topic string) uint
	GetDevices(facility string, users []string, devices []Device) error
}

type pqDatabase struct {
	DB *sqlx.DB
}

func NewPQDatabase(db *sqlx.DB) *pqDatabase {
	return &pqDatabase{
		DB: db,
	}
}

func (db *pqDatabase) AddDevice(device *Device) error {
	device.ID = DeviceUUID(device)
	q := `
		INSERT INTO twopush_devices
			(id, facility, user_id, device_token, device_provider)
		VALUES
			(:id, :facility, :user_id, :device_token, :device_provider)
	`
	_, err := db.DB.NamedExec(q, device)
	if err != nil {
		if pErr, ok := err.(*pq.Error); ok {
			if pErr.Code == pqErrUniqueConstraintViolation {
				return ErrDeviceExists
			}
		}
	}
	return err
}

func (db *pqDatabase) RemoveDevice(id *uuid.Uuid) error {
	res := db.DB.MustExec("DELETE FROM twopush_devices WHERE id = $1", id)
	if _, err := res.RowsAffected(); err != nil {
		return err
	}
	return nil
}

func (db *pqDatabase) RemoveDeviceToken(facility string, token string) error {
	q := "DELETE FROM twopush_devices WHERE facility = $1 AND device_token=$2"
	res := db.DB.MustExec(q, facility, token)
	if _, err := res.RowsAffected(); err != nil {
		return err
	}
	return nil
}

func (db *pqDatabase) RemoveUserDevices(facility string, userID string) error {
	q := "DELETE FROM twopush_devices WHERE facility = $1 AND user_id=$2"
	res := db.DB.MustExec(q, facility, userID)
	if _, err := res.RowsAffected(); err != nil {
		return err
	}
	return nil
}

func (db *pqDatabase) AddInterest(i *Interest) error {
	i.TopicUID = TopicUUID(i.Topic)
	q := `
		INSERT INTO twopush_subs
			(facility, user_id, topic_uid, topic, expires)
		VALUES
			(:facility, :user_id, :topic_uid, :topic, :expires)
	`
	_, err := db.DB.NamedExec(q, i)
	if err != nil {
		if pErr, ok := err.(*pq.Error); ok {
			if pErr.Code == pqErrUniqueConstraintViolation {
				return ErrInterestExists
			}
		}
	}
	return nil
}

func (db *pqDatabase) RemoveTopic(facility string, topic string) error {
	q := "DELETE FROM twopush_subs WHERE facility = $1 AND topic_uid = $2"
	res := db.DB.MustExec(q, facility, TopicUUID(topic))
	if _, err := res.RowsAffected(); err != nil {
		return err
	}
	return nil
}

func (db *pqDatabase) RemoveTopicForUser(
	facility,
	user string,
	topic string) error {

	q := "DELETE FROM twopush_subs WHERE facility = $1 AND user_id = $2 AND topic_uid = $3"
	res := db.DB.MustExec(q, facility, user, TopicUUID(topic))
	if _, err := res.RowsAffected(); err != nil {
		return err
	}
	return nil
}

func (db *pqDatabase) RemoveUserInterests(facility, user string) error {
	q := "DELETE FROM twopush_subs WHERE facility = $1 AND user_id = $2"
	res := db.DB.MustExec(q, facility, user)
	if _, err := res.RowsAffected(); err != nil {
		return err
	}
	return nil
}

func (db *pqDatabase) GetInterestedUsers(facility, topic string, intrs []Interest, offset, limit uint) error {
	q := `
		SELECT * FROM twopush_subs
		WHERE facility = $1 AND topic_uid = $2 AND (expires IS NULL OR expires > now())
	`

	if limit > 0 {
		q = q + fmt.Sprintf(" OFFSET %d LIMIT %d", offset, limit)
	}
	return db.DB.Select(&intrs, q, facility, TopicUUID(topic))
}

func (db *pqDatabase) GetInterestedUserCount(facility, topic string) uint {
	q := `
		SELECT COUNT(*) FROM twopush_subs
		WHERE facility = $1 AND topic_uid = $2 AND (expires IS NULL OR expires > now())
	`
	cnt := uint(0)
	db.DB.Get(&cnt, q, facility, TopicUUID(topic))
	return cnt
}

func (db *pqDatabase) GetDevices(facility string, users []string, devices []Device) error {
	q := `
		SELECT * FROM twopush_device
		WHERE facility = $1 AND users IN $2
	`
	return db.DB.Select(&devices, q, facility, users)
}
