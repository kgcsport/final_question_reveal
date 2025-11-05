# one_time_make_credentials.R
if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
pacman::p_load(tidyverse, bcrypt)

# Step 1. Your plain-text roster (temporary)
roster <- read_csv('general-resources/final_question_reveal/roster.csv') %>%
    rowwise() %>%
    mutate(pw_hash=bcrypt::hashpw(password)) %>%
    select(-password) %>%
    write_csv("general-resources/final_question_reveal/credentials.csv")

txt <- readChar("credentials.csv", file.info("credentials.csv")$size, useBytes = TRUE)
txt_b64 <- base64enc::base64encode(charToRaw(txt))
# add to Renviron
# Sys.setenv(CRED_B64=txt_b64)
cred_b64 <- Sys.getenv("CRED_B64")
cred_txt <- rawToChar(base64enc::base64decode(cred_b64))
cred_df <- read_csv(cred_txt)