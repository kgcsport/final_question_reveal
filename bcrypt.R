# one_time_make_credentials.R
if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
pacman::p_load(tidyverse, bcrypt)

# Step 1. Your plain-text roster (temporary)
roster <- read_csv('general-resources/final_question_reveal/roster.csv') %>%
    rowwise() %>%
    mutate(pw_hash=bcrypt::hashpw(password)) %>%
    select(-password) %>%
    write_csv("general-resources/final_question_reveal/credentials.csv")