library(dplyr)
library(tidyverse)
library(stringr)
library(lubridate)
library(httr)
library(purrr)
library(smartabaseR)
library(zoo)

##### Get User Info from Athlete 360. User ID will be required to import data into site.
username <- Sys.getenv("SB_USERNAME")
password <- Sys.getenv("SB_PASSWORD")

usss_athletes <- sb_get_user(
  url = "https://usopc.smartabase.com/athlete360-usss/",
  username = username,
  password = password,
  filter = sb_get_user_filter(
    user_key = "group",
    user_value = "U.S. Ski & Snowboard Athletes"
  )
)

##### Get all Firstbeat Data


#### using test csv right now
DataUpload <- read_csv('firstbeat_data.csv', show_col_types = FALSE) 
DataUpload <- DataUpload %>%
  select(-c("...1")) %>%
  mutate(
    Date = as.Date(Date),
    start_time = format(strptime(start_time, "%H:%M:%S"), "%I:%M %p"),
    end_time = format(strptime(end_time, "%H:%M:%S"), "%I:%M %p")
  )

# format dates correclty
DataUpload$start_time <- sub("^0", "", DataUpload$start_time)
DataUpload$end_time <- sub("^0", "", DataUpload$end_time)
DataUpload$Time <- DataUpload$end_time

# add user Ids onto data
DataUpload <- DataUpload %>%
  left_join(
    usss_athletes %>%
      select(first_name, last_name, user_id),
    by = c(
      "First Name" = "first_name",
      "Last Name"  = "last_name"
    )
  )

##### Create or Update event using Teamworks API
sb_insert_event(
  df = DataUpload,
  form = "Firstbeat Summary Stats",
  url = "https://usopc.smartabase.com/athlete360-usss",
  username = username,
  password = password,
  option = sb_insert_event_option(
    id_col = "user_id"
  )
)

###### delete csv
csv_path <- "firstbeat_data.csv"
if (file.exists(csv_path)) {
  file.remove(csv_path)
}
