# Scrape Player info and use pattern for BBALLREF player id creation


library(sqldf)
library(DBI)
library(RSQLite)
library(tidyverse)
library(furrr)
library(Rcpp)
library(dbplyr)
memory.limit(63000)

# letter page
url.letter <- "https://www.basketball-reference.com/players/{letter}/"

active.letters <- letters[-24] # all letters except x
rm(letters)


url.all.letters <- map(.x = c(active.letters),
            .f = function(x){gsub(x = url.letter, pattern = "\\{letter\\}", 
                                  replacement = x)}) %>% 
  unlist


NBA_Player_Profiles <- map_dfr(.x = url.all.letters,
                               .f = function(x){ Sys.sleep(1); cat(x); 
                                df <-  read_html(x) %>% 
                                        html_table()
                                
                                df
                               })


# With all data, feature pattern for bballref playerid

# 1 all names are first five letters of last with first two letters of first and 01 unless same then oldest is 01 and next oldest is 02 etc. 

# player page
"https://www.basketball-reference.com/players/a/abdelal01.html"


# Feature playerid and link to player page on bball ref -------------------

player.profile <- NBA_Player_Profiles %>% 
  mutate(Player = iconv(Player, from = 'UTF-8', to = 'ASCII//TRANSLIT')) %>% # get rid of crazy characters 
  separate(Player, sep = " ", into = c("first", "last"), remove = F, extra = "merge") %>% 
  mutate(last = ifelse(Player == "Nene", "Hilario", last)) %>% # nene only jerk with no last name but it's hilario
  mutate(last = str_replace_all(last,
                                  "( Sr$| I$| II$| III$| SR$| Jr.$| IV$)", "")) %>% 
  mutate(last = str_replace_all(last, "[\\*\\-.' ]", "")) %>% 
  mutate(first = str_replace_all(first, "[\\.' ]", "")) %>% 
  mutate(block1 = tolower(str_sub(last, 1,5))) %>%  # take first five letters of last name for block 1 of the id
  mutate(block2 = tolower(str_sub(first, 1,2))) %>% 
  mutate(block3 = paste0(block1, block2)) %>% 
  group_by(block3) %>% # now we need to order them by first year in db so we can assign numbers to duplicates
  arrange(From) %>% 
  mutate(number = row_number()) %>% 
  mutate(playerid = paste0(block3,"0",number)) %>% 
  mutate(letter = tolower(str_sub(last, 1, 1))) %>% # need letter of last lane for link
  mutate(link = paste0("https://www.basketball-reference.com/players/",letter,"/",playerid,".html")) %>% 
  ungroup() %>% 
  separate(Colleges, sep = ",", into = c("college", "college2"), extra = "merge") %>% 
  select(first, last, playerid, from = From, to = To, position = Pos, height = Ht, weight = Wt, 
         birth_date = `Birth Date`, college, college2, link) %>% 
  mutate(birth_date = lubridate::mdy(birth_date))


# Test Created Links & Keep Active ----------------------------------------

# Create links vector
player.links <- player.profile %>% 
  select(link) %>% 
  # add ron artest
  # add_row(link = "https://www.basketball-reference.com/players/a/artesro01.html") %>% 
  # add_row(link = "https://www.basketball-reference.com/players/p/pleisti01.html") %>% 
  # add_row(link = "https://www.basketball-reference.com/players/p/pendeje02.html") %>% 
  pull()


# # Checks
# check_link <- sapply(player.links, http_error) %>%
#   as.data.frame() 
# 
# #this eliminates inacitve urls
# link_hit_index <- which(check_link == FALSE)
# 
# #now use the index to filter through all the possible urls and keep active ones only. save as rds for safekeeping
# url_final <- player.links[link_hit_index]

# now to scrape from links ----------------------------------------------------------

library(tictoc)

tic()
NBA_Player_Seasons <- map_dfr(.x = player.links,
                              .f = function(x){ Sys.sleep(.5); cat(1);
                              tryCatch({
                               df <- read_html(x) %>% 
                                      html_table() %>% 
                                      .[[3]] %>% 
                                      janitor::clean_names()
                                
                              df$id = str_match(x,".*?/(\\w+).html")[,2]
                              
                             
                             
                             df$name <- as.character(read_html(x) %>% 
                                                 html_node("#meta > div > h1 > span") %>% 
                                                 html_text())
                             
                           # pull shooting hand
                            df$m1 <-  as.character(read_html(x) %>%
                               html_node("#meta > div > p:nth-child(1)") %>%
                               html_text())
                            
                            df$m1 <-  as.character(read_html(x) %>%
                                                     html_node("#meta > div > p:nth-child(1)") %>%
                                                     html_text())
                            
                            df$m2 <-  as.character(read_html(x) %>%
                                                     html_node("#meta > div > p:nth-child(2)") %>%
                                                     html_text())
                            
                            df$m3 <-  as.character(read_html(x) %>%
                                                     html_node("#meta > div > p:nth-child(3)") %>%
                                                     html_text())
                            
                            df$m4 <-  as.character(read_html(x) %>%
                                                     html_node("#meta > div > p:nth-child(4)") %>%
                                                     html_text())
                            
                            df$m5 <-  as.character(read_html(x) %>%
                                                     html_node("#meta > div > p:nth-child(5)") %>%
                                                     html_text())
                            
                            df$m6 <-  as.character(read_html(x) %>%
                                                     html_node("#meta > div > p:nth-child(6)") %>%
                                                     html_text())
                              
                            df
                              
                              }, error = function(e) message('Skipping url ', x))
})

toc()

saveRDS(NBA_Player_Seasons, file = "NBA Player Years Scrape.rds")



player.years <- NBA_Player_Seasons %>% 
  filter(!str_detect(season, "season"),
         !str_detect(season, "Career"),
         season != "") %>% 
  drop_na(id) %>% 
  select(playerid = id, name, season, team = tm, position = pos, age, league = lg, m1:m6) %>% 
  # grab shooting hand
  mutate(shooting.hand = case_when(
    str_detect(m2, "Shoots:\n  \n  Right\n") |
      str_detect(m3, "Shoots:\n  \n  Right\n") |
      str_detect(m4, "Shoots:\n  \n  Right\n") |
      str_detect(m5, "Shoots:\n  \n  Right\n") |
      str_detect(m6, "Shoots:\n  \n  Right\n") ~ "right",
    str_detect(m2, "Shoots:\n  \n  Left\n") |
      str_detect(m3, "Shoots:\n  \n  Left\n") |
      str_detect(m4, "Shoots:\n  \n  Left\n") |
      str_detect(m5, "Shoots:\n  \n  Left\n") |
      str_detect(m6, "Shoots:\n  \n  Left\n") ~ "left",
    TRUE ~ "unknown"
  )) %>% 
  select(-m1:-m6) %>% 
  # fix special characters
  mutate(name = str_replace_all(name, "ö", "o")) %>% 
  mutate(name = str_replace_all(name, "ü", "u")) %>% 
  mutate(name = iconv(name, from = 'UTF-8', to = 'ASCII//TRANSLIT')) %>% 
  # fix season to match style
  separate(season, sep = "-", into = c("s1", "s2")) %>% 
  mutate(s1.1 = as.character(str_sub(s1, 3,4)),
         season = paste0(s1.1,"/",s2)) %>% 
  select(-s1, -s2, -s1.1) %>% 
  # make short name for join
  separate(name, sep = " ", into = c("first", "last"), extra = "merge") %>% 
  mutate(last = str_replace_all(last,
                                      "( Sr$| I$| II$| III$| SR.$| Sr.$| Jr.$| IV$)", "")) %>% 
  # fix nene
  mutate(last = ifelse(first=="Nene", "Hilario", last)) %>% 
  mutate(short_name = paste0(str_sub(first, 1,1),". ",last)) %>% 
  # fix waller-prince
  mutate(short_name = ifelse(short_name == "T. Prince", "T. Waller-Prince", short_name)) %>% 
  mutate(short_name = ifelse(short_name == "D. Mbenga", "D. Ilunga-Mbenga", short_name))




# final to put into database
write_csv(player.years, file = "nba.player.years.csv")



# Patch Misjoined URLs ----------------------------------------------------

player.links <-  player.profile %>% 
  select(link) %>% 
  mutate(link = case_when(
    link == "https://www.basketball-reference.com/players/o/osmance01.html" ~
      "https://www.basketball-reference.com/players/o/osmande01.html",
    link == "https://www.basketball-reference.com/players/n/ntilifr01.html" ~
      "https://www.basketball-reference.com/players/n/ntilila01.html",
    link == "https://www.basketball-reference.com/players/k/klebema01.html" ~ 
      "https://www.basketball-reference.com/players/k/klebima01.html",
    link == "https://www.basketball-reference.com/players/f/freeden01.html" ~
      "https://www.basketball-reference.com/players/k/kanteen01.html",
    link == "https://www.basketball-reference.com/players/h/howarma01.html" ~
      "https://www.basketball-reference.com/players/h/howarma02.html",
    link == "https://www.basketball-reference.com/players/c/capelcl01.html" ~
      "https://www.basketball-reference.com/players/c/capelca01.html",
    link == "https://www.basketball-reference.com/players/j/jonesma04.html" ~
      "https://www.basketball-reference.com/players/j/jonesma05.html",
    link == "https://www.basketball-reference.com/players/t/tilliki01.html" ~
      "https://www.basketball-reference.com/players/t/tilliki02.html",
    link == "https://www.basketball-reference.com/players/l/louzadi01.html" ~
      "https://www.basketball-reference.com/players/l/louzama01.html",
    link == "https://www.basketball-reference.com/players/j/johnsda05.html" ~
      "https://www.basketball-reference.com/players/j/johnsda08.html",
    link =="https://www.basketball-reference.com/players/j/jonesca02.html" ~
      "https://www.basketball-reference.com/players/j/jonesca03.html",
    link == "https://www.basketball-reference.com/players/w/willibr02.html" ~
      "https://www.basketball-reference.com/players/w/willibr03.html",
    link == "https://www.basketball-reference.com/players/b/bareajj01.html" ~
      "https://www.basketball-reference.com/players/b/bareajo01.html",
    link == "https://www.basketball-reference.com/players/b/burtode01.html" ~
      "https://www.basketball-reference.com/players/b/burtode02.html",
    link == "https://www.basketball-reference.com/players/m/michaja01.html" ~
      "https://www.basketball-reference.com/players/m/mcadoja01.html",
    link == "https://www.basketball-reference.com/players/m/munfoxa01.html" ~
      "https://www.basketball-reference.com/players/m/munfoxa02.html",
    link == "https://www.basketball-reference.com/players/h/hairspj01.html" ~
      "https://www.basketball-reference.com/players/h/hairspj02.html",
    link == "https://www.basketball-reference.com/players/d/datomgi01.html" ~
      "https://www.basketball-reference.com/players/d/datomlu01.html",
    link == "https://www.basketball-reference.com/players/w/walkehe01.html" ~
      "https://www.basketball-reference.com/players/w/walkebi01.html",
    link == "https://www.basketball-reference.com/players/l/luizfvi01.html" ~
      "https://www.basketball-reference.com/players/f/favervi01.html",
    link == "https://www.basketball-reference.com/players/p/pavlosa01.html" ~
      "https://www.basketball-reference.com/players/p/pavloal01.html",
    link == "https://www.basketball-reference.com/players/s/senemo01.html" ~
      "https://www.basketball-reference.com/players/s/senesa01.html",
    link == "https://www.basketball-reference.com/players/v/vinicma01.html" ~
      "https://www.basketball-reference.com/players/v/vincima01.html",
    link == "https://www.basketball-reference.com/players/j/johnrpe01.html" ~
      "https://www.basketball-reference.com/players/r/ramospe01.html",
    link == "https://www.basketball-reference.com/players/a/abdulma01.html" ~
      "https://www.basketball-reference.com/players/a/abdulma02.html",
    link == "https://www.basketball-reference.com/players/s/smithto01.html" ~
      "https://www.basketball-reference.com/players/s/smithto02.html",
    link == "https://www.basketball-reference.com/players/m/mure?gh01.html" ~
      "https://www.basketball-reference.com/players/m/muresgh01.html",
    link == "https://www.basketball-reference.com/players/l/lemorki01.html" ~
      "https://www.basketball-reference.com/players/g/garriki01.html",
    TRUE ~ link
  )) %>% 
  add_row(link = "https://www.basketball-reference.com/players/a/artesro01.html") %>%
  add_row(link = "https://www.basketball-reference.com/players/p/pleisti01.html") %>%
  add_row(link = "https://www.basketball-reference.com/players/p/pendeje02.html") %>%
  pull(link)

NBA_Player_Seasons <- map_dfr(.x = player.links,
                              .f = function(x){ Sys.sleep(.5); cat(1);
                                tryCatch({
                                  df <- read_html(x) %>% 
                                    html_table() %>% 
                                    .[[3]] %>% 
                                    janitor::clean_names()
                                  
                                  df$id = str_match(x,".*?/(\\w+).html")[,2]
                                  
                                  
                                  
                                  df$name <- as.character(read_html(x) %>% 
                                                            html_node("#meta > div > h1 > span") %>% 
                                                            html_text())
                                  
                                  # pull shooting hand
                                  df$m1 <-  as.character(read_html(x) %>%
                                                           html_node("#meta > div > p:nth-child(1)") %>%
                                                           html_text())
                                  
                                  df$m1 <-  as.character(read_html(x) %>%
                                                           html_node("#meta > div > p:nth-child(1)") %>%
                                                           html_text())
                                  
                                  df$m2 <-  as.character(read_html(x) %>%
                                                           html_node("#meta > div > p:nth-child(2)") %>%
                                                           html_text())
                                  
                                  df$m3 <-  as.character(read_html(x) %>%
                                                           html_node("#meta > div > p:nth-child(3)") %>%
                                                           html_text())
                                  
                                  df$m4 <-  as.character(read_html(x) %>%
                                                           html_node("#meta > div > p:nth-child(4)") %>%
                                                           html_text())
                                  
                                  df$m5 <-  as.character(read_html(x) %>%
                                                           html_node("#meta > div > p:nth-child(5)") %>%
                                                           html_text())
                                  
                                  df$m6 <-  as.character(read_html(x) %>%
                                                           html_node("#meta > div > p:nth-child(6)") %>%
                                                           html_text())
                                  
                                  df
                                  
                                }, error = function(e) message('Skipping url ', x))
                              })



