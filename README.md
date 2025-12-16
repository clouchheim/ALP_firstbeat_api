## Firstbeat - Athlete360 API

This repository contains scripts to pull down a given interval (2 hours) of Firstbeat data for all U.S. Ski and Snowboard Athletes. The purpose of this project was to get RMSSD, 
which was unavailable in the Smartabase-Firstbeat integration. The data for this automation can be found in the 'Firstbeat Summary Stats' form on Athlete360.
The files below are scheduled in crontab through a shell script, supplied here. This will be later moved to a server or another local computer. 

# Workflow

All 'measurements' are pulled from the Firstbeat API for the determined time interval (same as the run interval). If there are measurements, then the python script formats them and 
writes them to a csv. This csv is then read by the R script and written to Athlete360. 

# Notes

The data currently only includes ACWR and RMSSD, but the measurement_id is included, so it can be matched to full session data in 'Firstbeat Session.'
Note on time: since firstbeat is used around the world, the script is triggered and the data is written with UTC time. 
