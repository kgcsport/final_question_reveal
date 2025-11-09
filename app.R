# app.R — MWE: persistent SQLite on Posit Connect Cloud

# --- noisy (on purpose) so you can see what's happening in Logs ---
options(shiny.sanitize.errors = FALSE, shiny.fullstacktrace = TRUE)
logf <- function(...) { cat(format(Sys.time()), "-", paste(..., collapse=" "), "\n", file=stderr()); flush(stderr()) }

pacman::p_load(shiny, DBI, RSQLite, pool, tidyverse)

# 1) Pick a persistent, writable dir.
# On Posit Connect/Cloud, CONNECT_CONTENT_DIR points to per-app persistent storage.
safe_data_dir <- function() {
  d <- Sys.getenv("CONNECT_CONTENT_DIR", "")
  if (nzchar(d)) {
    d <- file.path(d, "data")
  } else {
    # Local dev fallback
    d <- tools::R_user_dir("sqlite_mwe", which = "data")
  }
  if (!dir.exists(d)) dir.create(d, recursive = TRUE, showWarnings = FALSE)
  d
}

db_path <- file.path(safe_data_dir(), "appdata.sqlite")
logf("DB path:", db_path)

# 2) Create a single process-scoped pool. Do NOT close it on each session end.
db <- pool::dbPool(RSQLite::SQLite(), dbname = db_path)
reg.finalizer(environment(), function(e) { try(pool::poolClose(db), silent = TRUE) }, onexit = TRUE)

# 3) Defensive pragmas (WAL can be finicky on some filesystems).
#    TRUNCATE/DELETE journals are safer; busy_timeout avoids "database is locked".
try(DBI::dbExecute(db, "PRAGMA journal_mode=TRUNCATE;"), silent = TRUE)
try(DBI::dbExecute(db, "PRAGMA synchronous=NORMAL;"), silent = TRUE)
try(DBI::dbExecute(db, "PRAGMA busy_timeout=5000;"), silent = TRUE)

# 4) Initialize schema (idempotent).
DBI::dbExecute(db, "
  CREATE TABLE IF NOT EXISTS events(
    id   INTEGER PRIMARY KEY,
    who  TEXT NOT NULL,
    ts   TEXT NOT NULL
  );
")

# MWE app: add a row; show row count; show last 10 rows
ui <- fluidPage(
  tags$h3("SQLite on Posit Connect Cloud — MWE"),
  verbatimTextOutput("info"),
  textInput("who", "Your name", value = "tester"),
  actionButton("add", "Add a row"),
  actionButton("clear", "Clear all rows"),
  tags$hr(),
  strong("Row count:"),
  textOutput("nrows"),
  tags$h4("Last 10 rows"),
  tableOutput("tbl")
)

server <- function(input, output, session) {
  logf("SESSION start", session$token)

  output$info <- renderText({
    paste0("DB path: ", db_path, "\nFile exists: ", file.exists(db_path))
  })

  observeEvent(input$add, {
    nm <- if (nzchar(input$who)) input$who else "anon"
    DBI::dbExecute(db, "INSERT INTO events(who, ts) VALUES (?, datetime('now'))", params = list(nm))
    logf("Inserted row for", nm)
  })

  observeEvent(input$clear, {
    DBI::dbExecute(db, "DELETE FROM events")
    logf("Cleared all rows")
  })

  output$nrows <- renderText({
    # re-run reactively when add/clear pressed
    input$add; input$clear
    as.character(DBI::dbGetQuery(db, "SELECT COUNT(*) AS n FROM events")$n)
  })

  output$tbl <- renderTable({
    input$add; input$clear
    DBI::dbGetQuery(db, "SELECT * FROM events ORDER BY id DESC LIMIT 10")
  }, striped = TRUE, bordered = TRUE)

  session$onSessionEnded(function() logf("SESSION stop", session$token))
}

shinyApp(ui, server)
