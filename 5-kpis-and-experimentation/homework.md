# Homework - Experimentation

**Product:** Duolingo

**User Journey Until the Present Moment**

I started Duolingo because it felt low-friction—bite-sized lessons, instant feedback, and a cute, welcoming UI—but I stayed for the gamification: XP, streaks, and especially the weekly leagues that made me surprisingly competitive. The social features—adding friends, seeing streaks, nudging and challenging each other—turned it into a shared game. Over time the lessons evolved from basic drills to dynamic, contextual scenarios that feel closer to real conversations. Even the ads improved with playful banter, so they’re less intrusive. Now it’s a quick daily habit: keep the streak, climb the league, and progress alongside friends.

## Experiment No.1: Words Hub “Search & Categories”

Users can view learned words in the Practice Hub, but there’s no search and items are only alphabetically listed without grammatical/topic categories (e.g., verbs, pronouns, possessives). Add a search bar, filters by category, and tags on each word to help users find/review words when they need them, aiming to increase out-of-lesson engagement and overall stickiness.

### Null hypothesis (H0)
Adding search, filters, and categories to the Words section has no effect on out-of-lesson engagement, incremental session duration (beyond lesson time), or retention versus control.

### Alternative hypothesis (H1)
The enhanced Words section increases out-of-lesson engagement and incremental session duration, and improves retention versus control.

### Leading metrics
Incremental session duration outside lessons per user/day (Practice Hub time minus lesson time).
Words feature engagement: % of DAU using Words, searches/user, filter usage rate, word detail views/user, revisit rate to the same word within 7 days.
Sessions started from the home screen that include a Words interaction.

### Lagging metrics impacted
Retention: D7 and D28.
Streak continuation rate (7-day streak survival).
Subscription conversion/upsell rate (if applicable).

### Test cell allocation
Always 50% / 50% (Control: existing Words list; Treatment: Words with search + categories + tags).

## Experiment No.2: “Come-Back” Special Practice Lessons
Free users often drop or don’t finish lessons due to hearts and difficulty. When a user becomes inactive (e.g., ≥7 days), show a personalised, heart-friendly “Special Practice” lesson targeting their weakest skills to make re-entry easier and boost re-engagement.

### Null hypothesis (H0)
Special Practice lessons do not increase resurrection or post-return engagement versus a standard come-back to a normal lesson.

### Alternative hypothesis (H1)
Special Practice lessons increase resurrection and lead to longer post-return engagement versus the normal lesson.

### Leading metrics impacted
Resurrection rate: % of eligible inactive users who return and start any learning activity within 7 days of the prompt.
Sessions after resurrection: average # sessions in the 7 days after first return.
Lesson-level: start rate, completion rate of the prompted lesson, time to first action, heart losses per session (guardrail).

### Lagging metrics impacted
Resurrected WAU / MAU.
D7 / D28 retention from resurrection event.
Streak resumption rate; subscription conversion within 14/30 days (guardrails).

### Test cell allocation
Always 50% / 50% at user level among eligible inactive users.

## Experiment No.3: Live Sessions (Premium Upsell)
Introduce a 1:1 “Live Sessions” feature (short, level-matched practice for paid users) and promote it to free users in order to increase premium sign-ups and post-upgrade engagement.

### Null hypothesis (H0)
Exposure to Live Sessions and its promotion leads to no increase in premium sign-ups versus control.

### Alternative hypothesis (H1)
Exposure to Live Sessions and its promotion increases premium sign-ups; among new/upgraded premium users it boosts activation and engagement.

### Leading metrics impacted
Premium sign-up conversion rate among exposed free users.
Promo CTR (home card/paywall slot/push → Live Sessions info).
Feature activation: % of new premium users who start ≥1 live session within 7 days.
Engagement: live sessions/user/week, avg minutes per session, completion rate.


### Lagging metrics impacted
MRR uplift vs control (net of refunds).
Retention of newly converted premium: D7/D28 from upgrade.
Churn rate among existing premium (no adverse impact).
Streak survival for converters (7-day survival).

### Test cell allocation
Always 50% / 50% at the user level among eligible free users (supported languages with sufficient supply, 18+, supported geos).
