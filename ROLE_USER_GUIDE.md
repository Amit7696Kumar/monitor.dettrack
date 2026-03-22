# DET Monitoring Application User Guide

This guide explains how each role uses the application:

- `Admin`
- `Coadmin`
- `User`

It is written for day-to-day operations, not for development.

## Access and Login

1. Open the application in your browser.
2. Sign in with your assigned account.
3. After login, the system opens the dashboard for your role.

Main role landing pages:

- `Admin`: `/admin`
- `Coadmin`: `/coadmin/{team_id}`
- `User`: `/`

## Common Navigation

Most roles use these sections:

- `Dashboard`: main operational view
- `Tasks`: create, manage, or respond to assigned work
- `Alerts`: review active alerts and mark them read
- `Messages`: open the in-app chat popup
- `Settings`: account and assignment-related actions

Mobile and desktop show the same functions with different layouts.

## Admin Guide

### What Admin Can Do

Admin has the widest visibility in the system and can:

- View all teams
- Review all readings and task submissions
- Monitor alerts across the system
- Create and assign tasks
- Open uploaded evidence files
- Chat with users and coadmins
- Assign unassigned users to teams from Settings

### Admin Pages

- `Dashboard`: `/admin`
- `Teams`: `/teams`
- `Tasks`: `/tasks`
- `Alerts`: `/alerts`
- `Settings`: `/settings`

### Admin Dashboard

The Admin Dashboard is the main control center.

Use it to:

- Search readings by task, user, or value
- Filter readings by task title
- Review reading status
- Open uploaded evidence images/files
- Track unread alerts

On mobile:

- The dashboard appears as cards
- Use the top header to open `Alerts`, `Teams`, and `Profile/Settings`
- Use the bottom navigation for `Tasks`, `Dashboard`, `Messages`, and `Settings`

### View Teams

Open `/teams` to see all teams available in the system.

Use this page to:

- View the list of teams
- Open the team dashboard for each team
- Move quickly from admin-level view to team-level operations

### Create and Manage Tasks

Open `/tasks`.

Admin can:

- Create task forms
- Assign tasks to specific users
- Assign tasks to a team
- Set response type and deadline
- Enable or disable AI-related task behavior if available in the form
- Review task responses and evidence

### Alerts

Open `/alerts`.

Admin can:

- Review all system alerts
- Mark alerts as read
- Clear alerts when appropriate

### Settings

Open `/settings`.

Admin can:

- Assign unassigned users to any available team
- Review role/team-related assignment options

## Coadmin Guide

### What Coadmin Can Do

Coadmin manages one team and can:

- View only their team dashboard
- Review team readings and task submissions
- Monitor team alerts
- Create or assign tasks for their team
- View team members
- Chat with users and admins
- Add unassigned users into their own team from Settings

### Coadmin Pages

- `Dashboard`: `/coadmin/{team_id}`
- `Members`: `/users`
- `Tasks`: `/tasks`
- `Alerts`: `/alerts`
- `Settings`: `/settings`

### Coadmin Dashboard

The coadmin dashboard is focused on a single team.

Use it to:

- Review team operations
- Search and filter readings
- Open uploaded evidence
- Check unread alerts
- Monitor task submission status

On mobile:

- The header shortcut for coadmin opens `Members`
- The member list should show team members relevant to that coadmin scope

### View Members

Open `/users`.

Use this page to:

- View team members
- Review who belongs to the current team scope

### Tasks

Open `/tasks`.

Coadmin can:

- Create tasks for users
- Assign tasks within the coadmin's allowed team scope
- Review submissions
- Track pending, submitted, late, and overdue work

### Alerts

Open `/alerts`.

Coadmin can:

- Review alerts for their own team
- Mark alerts as read
- Clear team alerts when needed

### Settings

Open `/settings`.

Coadmin can:

- Add unassigned users to their own team

## User Guide

### What User Can Do

User actions are narrower and task-focused.

Users can:

- Open their dashboard
- Review recent processed results
- Open assigned tasks
- Upload required response files through tasks
- Chat through the in-app messaging popup
- Open Settings

### User Pages

- `Dashboard`: `/`
- `My Tasks`: `/tasks`
- `Settings`: `/settings`

### User Dashboard

The user dashboard is mainly for:

- Reviewing recent processed results
- Opening uploaded image evidence where available
- Quickly moving to assigned tasks

Important:

- Direct upload is disabled on the dashboard
- Users submit work through `My Tasks`

### My Tasks

Open `/tasks`.

Users can:

- Review assigned work
- See status such as pending, submitted, or overdue
- Upload image, PDF, video, or other required evidence depending on the task
- Submit numeric/task-specific responses where required

### Messages

Use the `Open Messages` button or bottom navigation.

Users can:

- Send and receive messages
- Open direct or group conversations allowed by the system
- Read updates from admins/coadmins

### Settings

Open `/settings` to manage user-level account actions available in the current build.

## In-App Messaging

The messaging popup is available across the application.

Main features:

- Conversation list
- Search
- Direct and group chats where permitted
- Read status
- Reactions
- Edit/delete controls where allowed

Typical use cases:

- Admin to coadmin coordination
- Coadmin to team user follow-up
- User clarification on assigned tasks

## Reading and Evidence Review

Where readings or task submissions are shown, the `Open` action is used to inspect uploaded evidence.

You may see:

- Uploaded image/file
- OCR-related debug outputs, depending on the page and role

## Alerts and Status Handling

Statuses commonly shown in dashboards and tasks include:

- `Pending`
- `Submitted`
- `Completed`
- `Late`
- `Overdue`

Recommended practice:

- Review `Overdue` items first
- Clear or mark alerts only after verification

## Mobile Usage Notes

On mobile:

- Dashboards are optimized into cards instead of wide tables
- Bottom navigation is used for the most common sections
- Top header shortcuts provide quick access to alerts, team/member pages, and settings where applicable

If a UI change does not appear immediately on mobile:

1. Refresh the page.
2. Close and reopen the browser tab or installed app.
3. Reopen the application after the server reloads.

## Quick Role Summary

### Admin

- Scope: all teams
- Key pages: `/admin`, `/teams`, `/tasks`, `/alerts`, `/settings`

### Coadmin

- Scope: one assigned team
- Key pages: `/coadmin/{team_id}`, `/users`, `/tasks`, `/alerts`, `/settings`

### User

- Scope: own tasks and own workspace
- Key pages: `/`, `/tasks`, `/settings`

## Troubleshooting

### I cannot find my tasks

- Open `/tasks`
- Check whether the task was assigned to your user or team

### I cannot see uploaded evidence

- Use the `Open` button where available
- If nothing opens, confirm a file was actually attached to that record

### I cannot see recent UI changes on mobile

- Refresh the page
- Reopen the app/tab
- Make sure the server has reloaded after the latest update

### I cannot see a team or member list

- Admin should use `/teams`
- Coadmin should use `/users`
- Visibility depends on role permissions and assigned scope
