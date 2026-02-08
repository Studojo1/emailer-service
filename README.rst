Emailer Service
===============

Production-ready email service using Azure Communication Services Email.

Features
--------

- Sends transactional emails via Azure Email Communication Services
- HTML email templates matching frontend design system
- Event-driven email triggers (signup, resume optimization, internship applications)
- Forgot password flow with secure token generation
- User email preferences management
- Respects user opt-out preferences (except security emails)

Email Types
-----------

1. **Welcome Email** - Sent after successful user signup
2. **Forgot Password** - Password reset with secure token
3. **Resume Optimized** - Notification when resume optimization completes
4. **Internship Applied** - Confirmation when user applies to an internship

Architecture
------------

- **Language**: Go
- **Port**: 8087
- **Database**: PostgreSQL (shared with frontend)
- **Messaging**: RabbitMQ (cp.events exchange)
- **Email Provider**: Azure Communication Services Email

Endpoints
---------

- `GET /health` - Health check
- `POST /v1/email/forgot-password` - Request password reset
- `POST /v1/email/reset-password` - Confirm password reset
- `GET /v1/email/preferences/:user_id` - Get email preferences
- `PUT /v1/email/preferences/:user_id` - Update email preferences
- `POST /v1/email/events` - Publish email event (internal)

Environment Variables
---------------------

- `DATABASE_URL` - PostgreSQL connection string
- `RABBITMQ_URL` - RabbitMQ connection string
- `AZURE_COMMUNICATION_SERVICE_CONNECTION_STRING` - Azure Email connection string
- `AZURE_EMAIL_SENDER_ADDRESS` - Sender email (default: no-reply@studojo.com)
- `FRONTEND_URL` - Frontend URL for email links
- `HTTP_PORT` - HTTP server port (default: 8087)
- `TEMPLATE_DIR` - Template directory (default: /app/templates)

Deployment
----------

Deployed to Kubernetes in the `studojo` namespace.

See `k8s/emailer-service/deployment.yaml` for deployment configuration.

