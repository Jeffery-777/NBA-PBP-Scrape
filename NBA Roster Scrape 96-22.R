
# Scrape a player's table

library(rvest)
library(tidyverse)
library(curl)
library(tidyverse)
library(httr)
memory.limit(80000)

url <- "https://www.basketball-reference.com/teams/{home_team}/{year}.html"

tm_abr <- c("ATL","BOS","BRK","NJN","CHA", "CHH","CHO","CHI",
            "CLE","DAL","DEN","DET","GSW","HOU","IND",
            "LAC","LAL","MEM","MIA","MIL","MIN","NOH", "NOK",
            "NOP","NYK","OKC","ORL","PHI","PHO","POR",
            "SEA","SAC","SAS","TOR","UTA","VAN", "WAS","WSB")

yrs <- c(1996:2022)

url2 <- map(.x = c(tm_abr),
            .f = function(x){gsub(x = url, pattern = "\\{home_team\\}", 
                                  replacement = x)}) %>% 
  unlist

url3 <- map(.x = c(yrs),
            .f = function(x){gsub(x = url2, pattern = "\\{year}", 
                                  replacement = x)}) %>% 
  unlist

#this creates an object of the indices of active urls
check_link <- sapply(url3, http_error) %>%
  as.data.frame() 

#this eliminates inacitve urls
link_hit_index <- which(check_link == FALSE)


#now use the index to filter through all the possible urls and keep active ones only. save as rds for safekeeping
url_final <- url3[link_hit_index]

nba_rosters <- map_dfr(.x = url_final,
                    .f = function(x){ cat(x); 
                      df <- read_html(curl(x, handle = curl::new_handle("useragent" = "Mozilla/5.0"))) %>% 
                        html_nodes("table") %>% 
                        html_table(fill = T) %>%
                        .[[1]] %>% 
                        janitor::clean_names() 
                      
                      df$exp <- as.character(df$exp)
                      df$no <- as.character(df$no)
                  
                      c <- read_html(x) %>%
                        html_nodes(xpath = "//span[(((count(preceding-sibling::*) + 1) = 1) and parent::*)]") %>%
                        .[5] %>% 
                        as.character()
                      
                      df$team2 <- c
                      df$url <- x

                      
                      df 
                      
                    })


# Clean Up Table ----------------------------------------------------------

roster.cleaned <- nba_rosters %>% 
  separate(team2, sep = "\\>", into = c("t1", "t2")) %>% 
  separate(t2, sep = "\\<", into = c("team", "trash")) %>% 
  mutate(team_abbr = str_sub(url, 44,46)) %>% 
  mutate(season1 = as.numeric(str_sub(url, 50,51))-1) %>% 
  mutate(season2 = season1 + 1) %>% 
  mutate(season1.pad = str_pad(season1, 2, pad = "0"),
         season2.pad = str_pad(season2, 2, pad = "0")) %>% 
  mutate(season = paste0(str_sub(season1.pad, 1,2),"-", str_sub(season2.pad, 1,2))) %>% 
  separate(no, sep = ",", into = c("number", "number_alt")) %>% 
  mutate(experience = ifelse(exp == "R", 0, exp)) %>% 
  separate(ht, sep = "-", into = c("feet", "inches"), remove = FALSE) %>% 
  mutate(bday = lubridate::mdy(birth_date)) %>% 
  separate(player, sep = " ", into = c("first", "last", "extra"), remove = FALSE) %>% 
  mutate(extra = ifelse(is.na(extra),"", extra)) %>% 
  mutate(short_name = paste0(str_sub(first, 1,1),". ",last, extra)) %>% 
  mutate(tm.spaces = str_count(team, " ")) %>% 
  mutate(city = ifelse(tm.spaces >= 2, 
                       str_match(team, "(.*? .*?) .*")[,2], 
                       str_match(team, "(.*?) .*")[,2])) %>% 
  mutate(season = str_replace(season, "-","/")) %>% 
  mutate(season = ifelse(season == "/1-00", "99/00", season)) %>% 
  select(season, player, short_name, number, number_alt, position = pos, team, team_abbr, city, height = ht, feet, inches, weight = wt, birth_date, bday, experience, college)

write.csv(roster.cleaned, file = "NBA Roster 95-22.csv")
 


