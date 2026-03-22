# DET Monitoring Application - Launching Document

## Public URL
- `https://monitor.dettrack.com`

## Purpose
This document is for go-live usage by:
- Admin
- Co-admin
- User

It covers login access, daily workflow, and basic operational checks.

## Roles and Access

### Admin
- Full access to platform
- Creates and assigns tasks
- Monitors all teams
- Reviews alerts and submitted evidence
- Uses messaging with coadmins/users

### Co-admin
- Team-level access
- Monitors assigned team dashboard
- Reviews alerts and submissions for team users
- Uses messaging

### User
- Task-only workflow
- Views assigned tasks
- Submits required image/video response once per task

## Login
1. Open `https://monitor.dettrack.com/login`
2. Enter assigned username and password
3. After login:
- Admin -> Admin Dashboard
- Co-admin -> Team Dashboard
- User -> Tasks page

## Admin Launch Checklist
1. Login and verify dashboard loads.
2. Confirm all 6 teams are visible.
3. Open `Tasks` and create a test task.
4. Assign task to a user/team.
5. Verify task appears for assigned user.
6. Open `Messages` popup and verify communication works.
7. Verify alert panel updates and unread count changes.
8. Verify table filtering by task title works.
9. Click `Open` in image column and confirm popup image view.

## Co-admin Launch Checklist
1. Login and verify team dashboard loads.
2. Verify `Total Task`, `Total Alerts`, and `Unread Alerts` counts.
3. Verify merged table shows assigned team items.
4. Use filter dropdown and confirm task-title filtering works.
5. Click `Open` and verify image popup.
- For odometer tasks, verify Start + End images both appear.
6. Check Alerts tab and test `Clear Alerts`.
7. Verify messaging popup opens and sends messages.

## User Launch Checklist
1. Login and confirm user lands on `/tasks`.
2. Verify assigned tasks are listed with latest on top.
3. Confirm user can submit response only once per task.
4. For Odometer Reading task:
- Upload Start Image
- Upload End Image
- Submit
5. Verify submitted task hides submit controls (shows already submitted state).

## Task Workflow (Live)
1. Admin/Coadmin creates task using Task Title dropdown.
2. User submits task response.
3. System processes submission:
- Earthing: extracts value from image and checks ideal value
- Odometer: reads start/end values, computes distance and fuel used
4. Dashboard tables populate status/value/image.
5. Alerts are generated for threshold violations or overdue tasks.

## Odometer Task Rules
- Task title must be `Odometer Reading`.
- User uploads 2 images:
  - Start Image
  - End Image
- Average KMPL is taken from task configuration.
- System computes:
  - Distance difference
  - Fuel consumed (liters)

## Messaging
- Available for Admin, Co-admin, and User via `Open Messages` button.
- Opens in popup on same page.

## Basic Troubleshooting

### If filters do not update
- Hard refresh browser: `Cmd + Shift + R` (Mac) or `Ctrl + Shift + R` (Windows)

### If Open button does not show image
- Confirm row has uploaded image path.
- Re-login and retry.

### If value is missing for earthing
- Ensure task title includes `Earthing`.
- Ensure image quality is clear and numeric display is readable.

### If odometer shows one image only
- Ensure both start and end images were uploaded in submission.

## Go-Live Notes
- Keep credentials secure and rotate default passwords.
- Restrict role access strictly by account.
- Review alerts daily.
- Ensure periodic backup of `server/meter.db` and `server/uploads/`.

## Support Escalation
When reporting issues, include:
- Role (Admin/Coadmin/User)
- Username
- Team number
- Task ID / row ID
- Timestamp of issue
- Screenshot (if possible)
