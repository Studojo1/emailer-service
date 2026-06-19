package email

import "context"

// SendOpsAlert sends a plain internal ops alert (no template, no tracking, no
// unsubscribe) from the support domain. Used for operational tripwires such as
// the Apollo credit-burn alarm — not user-facing marketing/transactional mail.
func (s *Sender) SendOpsAlert(ctx context.Context, to, subject, htmlContent string) error {
	from := s.supportSender
	if from == "" {
		from = s.welcomeSender
	}
	return s.client.SendEmailFrom(ctx, from, to, subject, htmlContent)
}
