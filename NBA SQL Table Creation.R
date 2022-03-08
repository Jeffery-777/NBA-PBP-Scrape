library(sqldf)
library(DBI)
library(RSQLite)
library(tidyverse)
library(furrr)
library(Rcpp)
library(dbplyr)
library(nflfastR)
memory.limit(150000)


# Build SLQ Connection
connection <- dbConnect(SQLite(), "nba.data")
connection

dbListTables(connection)

nba_pbp <- dplyr::tbl(connection, "NBA PBP IDs")
nba_roster <- dplyr::tbl(connection, "NBA Rosters")
nba_players <- dplyr::tbl(connection, "NBA Players")
nba_player.years <- dplyr::tbl(connection, "NBA Player Seasons")
nba_city.initials <- dplyr::tbl(connection, "NBA City Initials")

# add unique player ids to all player name cols of pbp ------------------

nba_pbp_ids <- nba_pbp %>% 
  # left_join(nba_city.initials, by = c("possession" = "team")) %>% 
  # # join shooter
  # left_join(nba_player.years %>% 
  #             select(-league) %>% 
  #             rename(team_abbr = team,
  #                     shooter.playerid = playerid,
  #                     shooter.first = first,
  #                     shooter.last = last,
  #                     shooter.position = position,
  #                     shooter.age = age,
  #                     shooter.hand = shooting.hand),
  #           by = c("season", "player_fg" = "short_name", "abb" = "team")) %>% 
  # # join assist
  # left_join(nba_player.years %>% 
  #             select(-league, -shooting.hand) %>% 
  #             rename(assist.playerid = playerid,
  #                    assist.first = first,
  #                    assist.last = last,
  #                    assist.position = position,
  #                    assist.age = age),
  #           by = c("season", "player_assist" = "short_name", "abb" = "team" )) %>% 
  # join rebound
  left_join(nba_player.years %>% 
              select(-league, -shooting.hand) %>% 
              rename(rebound.d.playerid = playerid,
                     rebound.d.first = first,
                     rebound.d.last = last,
                     rebound.d.position = position,
                     rebound.d.age = age),
            by = c("season", "player_defensive_rebound" = "short_name", "abb" = "team" )) %>% 
  # join off rebound
  left_join(nba_player.years %>% 
              select(-league, -shooting.hand) %>% 
              rename(rebound.o.playerid = playerid,
                     rebound.o.first = first,
                     rebound.o.last = last,
                     rebound.o.position = position,
                     rebound.o.age = age),
            by = c("season", "player_offensive_rebound" = "short_name", "abb" = "team" )) %>% 
  # join steals
  left_join(nba_player.years %>% 
              select(-league, -shooting.hand) %>% 
              rename(steal.playerid = playerid,
                     steal.first = first,
                     steal.last = last,
                     steal.position = position,
                     steal.age = age),
            by = c("season", "player_steal" = "short_name", "abb" = "team" )) %>% 
  # join block
  left_join(nba_player.years %>% 
              select(-league, -shooting.hand) %>% 
              rename(block.playerid = playerid,
                     block.first = first,
                     block.last = last,
                     block.position = position,
                     block.age = age),
            by = c("season", "player_block" = "short_name", "abb" = "team" )) %>% 
  # join draw foul
  left_join(nba_player.years %>% 
              select(-league, -shooting.hand) %>% 
              rename(drawfoul.playerid = playerid,
                     drawfoul.first = first,
                     drawfoul.last = last,
                     drawfoul.position = position,
                     drawfoul.age = age),
            by = c("season", "player_draw_foul" = "short_name", "abb" = "team" )) %>% 
  # join foul
  left_join(nba_player.years %>% 
              select(-league, -shooting.hand) %>% 
              rename(foul.playerid = playerid,
                     foul.first = first,
                     foul.last = last,
                     foul.position = position,
                     foul.age = age),
            by = c("season", "player_foul" = "short_name", "abb" = "team" )) %>% 
  # join travel
  left_join(nba_player.years %>% 
              select(-league, -shooting.hand) %>% 
              rename(travel.playerid = playerid,
                     travel.first = first,
                     travel.last = last,
                     travel.position = position,
                     travel.age = age),
            by = c("season", "player_travelling" = "short_name", "abb" = "team" )) %>%
  # join turnover
  left_join(nba_player.years %>% 
              select(-league, -shooting.hand) %>% 
              rename(turnover.playerid = playerid,
                     turnover.first = first,
                     turnover.last = last,
                     turnover.position = position,
                     turnover.age = age),
            by = c("season", "player_turnover" = "short_name", "abb" = "team" )) %>%
  as.data.frame()
  


dbDisconnect(connection)


# CSVs from Github --------------------------------------------------------

nba_pbp_22_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202022.csv.gz")
nba_pbp_21_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202021.csv.gz")
nba_pbp_20_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202020.csv.gz")

nba_pbp_19_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202019.csv.gz")
nba_pbp_18_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202018.csv.gz")
nba_pbp_17_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202017.csv.gz")

nba_pbp_16_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202016.csv.gz")
nba_pbp_15_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202015.csv.gz")
nba_pbp_14_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202014.csv.gz")

nba_pbp_13_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202013.csv.gz")
nba_pbp_12_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202012.csv.gz")
nba_pbp_11_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202011.csv.gz")

nba_pbp_10_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202010.csv.gz")
nba_pbp_09_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202009.csv.gz")
nba_pbp_08_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202008.csv.gz")

nba_pbp_07_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202007.csv.gz")
nba_pbp_06_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202006.csv.gz")
nba_pbp_05_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202005.csv.gz")

nba_pbp_04_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202004.csv.gz")
nba_pbp_03_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202003.csv.gz")
nba_pbp_02_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202002.csv.gz")

nba_pbp_01_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202001.csv.gz")
nba_pbp_00_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%202000.csv.gz")
nba_pbp_99_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%201999.csv.gz")

nba_pbp_98_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%201998.csv.gz")
nba_pbp_97_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%201997.csv.gz")
nba_pbp_96_calendar <- read_csv("https://github.com/Jeffery-777/NBA-PBP-Scrape/raw/master/nba%20pbp%20calendar%201996.csv.gz")

# Binding years -----------------------------------------------------------

nba.all <- bind_rows(nba_pbp_96_calendar,
                     nba_pbp_97_calendar,
                     nba_pbp_98_calendar,
                     nba_pbp_99_calendar,
                     nba_pbp_00_calendar,
                     nba_pbp_01_calendar,
                     nba_pbp_02_calendar,
                     nba_pbp_03_calendar,
                     nba_pbp_04_calendar,
                     nba_pbp_05_calendar,
                     nba_pbp_06_calendar,
                     nba_pbp_07_calendar,
                     nba_pbp_08_calendar,
                     nba_pbp_09_calendar,
                     nba_pbp_10_calendar,
                     nba_pbp_11_calendar,
                     nba_pbp_12_calendar,
                     nba_pbp_13_calendar,
                     nba_pbp_15_calendar,
                     nba_pbp_16_calendar,
                     nba_pbp_17_calendar,
                     nba_pbp_18_calendar,
                     nba_pbp_19_calendar,
                     nba_pbp_20_calendar,
                     nba_pbp_21_calendar,
                     nba_pbp_22_calendar) 



# Creating SQL Tables -----------------------------------------------------


# NBA PBP Tables
# dbWriteTable(connection, "NBA PBP", nba.all, overwrite = TRUE)
# dbWriteTable(connection, "NBA Rosters", read.csv("NBA Roster 95-22.csv"), overwrite = TRUE)
dbWriteTable(connection, "NBA players", read.csv("nba.player.years.csv"), overwrite = TRUE)
dbWriteTable(connection, "NBA City Initials", nba_city_abb, overwrite = TRUE)
dbWriteTable(connection, "NBA Player Seasons", player.years, overwrite = TRUE)
dbWriteTable(connection, "NBA PBP IDs", nba_pbp_ids, overwrite = TRUE)

# dbAppendTable(connection, "NBA PBP", nba.all2)
# dbRemoveTable(connection, "NBA PBP")
# Make Tables Queriable ---------------------------------------------------




