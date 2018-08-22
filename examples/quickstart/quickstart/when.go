package quickstart

import mesg "github.com/mesg-foundation/go-application"

func (q *QuickStart) whenRequest() (*mesg.Listener, error) {
	return q.app.
		WhenEvent(q.config.WebhookServiceID, mesg.EventKeyCondition("request")).
		Map(func(*mesg.Event) mesg.Data {
			return sendgridRequest{
				Email:          q.config.Email,
				SendgridAPIKey: q.config.SendgridKey,
			}
		}).
		Execute(q.config.DiscordInvServiceID, "send")
}

func (q *QuickStart) whenDiscordSend() (*mesg.Listener, error) {
	return q.app.
		WhenResult(q.config.DiscordInvServiceID, mesg.TaskKeyCondition("send")).
		Filter(func(r *mesg.Result) bool {
			// we don't necessarily need to use a map here instead of using an interface{}.
			// this is just done to demonstrate to avoid task execution for
			// TestWhenResultFalseFilter test.
			var resp map[string]interface{}
			return r.Data(&resp) == nil
		}).
		Map(func(r *mesg.Result) mesg.Data {
			var resp interface{}
			r.Data(&resp)
			return logRequest{
				ServiceID: q.config.DiscordInvServiceID,
				Data:      resp,
			}
		}).
		Execute(q.config.LogServiceID, "log")
}

type sendgridRequest struct {
	Email          string `json:"email"`
	SendgridAPIKey string `json:"sendgridAPIKey"`
}

type logRequest struct {
	ServiceID string      `json:"serviceID"`
	Data      interface{} `json:"data"`
}
