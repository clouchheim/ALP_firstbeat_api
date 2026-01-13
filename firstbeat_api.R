# ensure R sees cached library
.libPaths(c(Sys.getenv("R_LIBS_USER"), .libPaths()))

library(dplyr)
library(readr)
library(smartabaseR)

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
csv_path <- "firstbeat_data.csv"

if (!file.exists(csv_path)) {
  message("No firstbeat_data.csv found. Nothing to upload.")
  quit(save = "no", status = 0)
}

DataUpload <- read_csv(csv_path, show_col_types = FALSE)

if (nrow(DataUpload) == 0) {
  message("firstbeat_data.csv exists but contains no rows. Nothing to upload.")
  quit(save = "no", status = 0)
}

DataUpload <- DataUpload %>%
  mutate(
    Date = as.Date(Date, format = "%m/%d/%Y"),
    start_time = format(strptime(start_time, "%H:%M:%S"), "%I:%M %p"),
    end_time   = format(strptime(end_time, "%H:%M:%S"), "%I:%M %p")
  )

DataUpload <- DataUpload %>%
  filter(!is.na(Date)) %>%
  filter(format(Date, "%Y") == "2026")

if (nrow(DataUpload) == 0) {
  message("No 2026 records found. Nothing to upload.")
  quit(save = "no", status = 0)
}

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


##### Remove duplicates

# get past day of measuerment IDs to avoid duplicates
past_measurements <- sb_get_event(
  form = "Firstbeat Summary Stats",
  date_range = sb_date_range("1", "days"),
  url = "https://usopc.smartabase.com/athlete360-usss",
  username = username,
  password = password
)

past_ids <- character(0)

if (!is.null(past_measurements) &&
    nrow(past_measurements) > 0 &&
    "ID" %in% names(past_measurements)) {
  past_ids <- past_measurements$ID
}

DataUpload <- DataUpload %>%
  filter(!ID %in% past_ids)

if (nrow(DataUpload) == 0) {
  message("All records are duplicates. Nothing to upload.")
  quit(save = "no", status = 0)
}

##### Create or Update event using Teamworks API

DataUpload <- DataUpload %>%
  select(-`First Name`, -`Last Name`)

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
#csv_path <- "firstbeat_data.csv"

#if (file.exists(csv_path)) {
  #file.remove(csv_path)
#}

print("Successfully uploaded Firstbeat data and deleted local CSV.")
