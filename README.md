#LastFM Sessionizer

## Prerequisite
Download last fm dataset from http://mtg.upf.edu/static/datasets/last.fm/lastfm-dataset-1K.tar.gz

## Problem Statement
Say we define a user’s “session” of Last.fm usage to be comprised of one or more songs played by that user, where each song is started within 20 minutes of the previous song’s start time.

Create a list of the top 10 longest sessions, with the following information about each session: userID, timestamp of first and last songs in the session, and the list of songs played in the session (in order of play).