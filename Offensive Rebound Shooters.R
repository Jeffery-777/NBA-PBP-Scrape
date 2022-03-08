
# Player with most Shots resulting in offensive rebounds ------------------

load(tidyverse)
connection <- dbConnect(SQLite(), "nba.data")

nba_pbp <- dplyr::tbl(connection, "NBA PBP IDs")


# part 1 ------------------------------------------------------------------


# where player_offensive_Rebound is not na

# season
or.leaders <- nba_pbp %>% 
  select(season, player_fg, shooter.playerid, shooter.position, shooter.hand, player_offensive_rebound, shooter.first, shooter.last) %>% 
  mutate(shot.result.off.rb = ifelse(!is.na(lead(player_offensive_rebound)), 1, 0)) %>% 
  mutate(self.rb = ifelse(player_fg == lead(player_offensive_rebound), 1, 0)) %>% 
  group_by(player_fg, shooter.playerid, shooter.position, shooter.first, shooter.last, shooter.hand) %>% 
  summarise(off.rb.shots = sum(shot.result.off.rb, na.rm = T),
            self.rb = sum(self.rb, na.rm = T),
            shots.total = n()) %>% 
  mutate(qual.shots = off.rb.shots - self.rb) %>% 
  mutate(off.rb.per = qual.shots / shots.total) %>% 
  filter(shots.total >= 100) %>% 
  ungroup() %>% 
  filter(!is.na(player_fg)) %>% 
  arrange(desc(off.rb.per)) %>% 
  head(100) %>% 
  as.data.frame()

# awesome. most off rebounds that weren't from the shooter




# let's look into Bo Outlaw. He was in the league a long time and probably shouldn't have shot long shots

nba_pbp %>% 
  filter(player_fg=="B. Outlaw") %>% 
  select(fg_length) %>% 
  ggplot(aes(fg_length))+
  geom_histogram()

# Bo lived up to his name and like an outlaw went out of his role to throw wild bricks at the basket


nba_pbp %>% 
  filter(player_fg=="B. Outlaw",
         fg_length >= 10) %>% 
  count()
