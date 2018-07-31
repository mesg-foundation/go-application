package quickstart

import mesg "github.com/mesg-foundation/go-application"

func (q *QuickStart) whenRequest() (*mesg.Stream, error) {
	return q.app.
		WhenEvent(q.config.WebhookServiceID, mesg.EventFilterOption("request")).
		Map(sendgridRequest{
			Email:          q.config.Email,
			SendgridAPIKey: q.config.SendgridKey,
		}).
		Execute(q.config.DiscordInvServiceID, "send")
}

func (q *QuickStart) whenDiscordSend() (*mesg.Stream, error) {
	return q.app.
		WhenResult(q.config.DiscordInvServiceID, mesg.TaskFilterOption("send")).
		FilterFunc(func(r *mesg.Result) bool {
			var resp interface{}
			return r.Decode(&resp) == nil
		}).
		MapFunc(func(r *mesg.Result) mesg.Data {
			var resp interface{}
			r.Decode(&resp)
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
