# app.R - Minimal FD leak tester
library(shiny)
library(DBI)
library(RSQLite)

## ---- Global setup ----

DB_DIR  <- "data"
DB_PATH <- file.path(DB_DIR, "appdata.sqlite")

if (!dir.exists(DB_DIR)) dir.create(DB_DIR, recursive = TRUE, showWarnings = FALSE)

# Single global connection, no pool
conn <- dbConnect(SQLite(), DB_PATH)

# Make sure we close it when the R process exits
onStop(function() {
  try(dbDisconnect(conn), silent = TRUE)
})

# Create a tiny pledges table if it doesn't exist
dbExecute(conn, "
  CREATE TABLE IF NOT EXISTS pledges (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    amount       REAL,
    submitted_at TEXT DEFAULT CURRENT_TIMESTAMP
  );
")

## ---- UI ----

ui <- fluidPage(
  titlePanel("FD Leak MWE"),

  sidebarLayout(
    sidebarPanel(
      numericInput("pledge", "Pledge amount", value = 0, min = 0),
      actionButton("submit", "Submit pledge"),
      tags$hr(),
      verbatimTextOutput("status")
    ),
    mainPanel(
      h4("Instructions"),
      tags$ol(
        tags$li("Deploy this app to the same Posit Connect environment."),
        tags$li("Watch the server logs over time."),
        tags$li("FD count should stay roughly flat if there's no leak.")
      )
    )
  )
)

## ---- Server ----

server <- function(input, output, session) {

  # Insert a row on submit
  observeEvent(input$submit, {
    amt <- suppressWarnings(as.numeric(input$pledge))
    if (!is.finite(amt) || amt < 0) return()

    dbExecute(
      conn,
      "INSERT INTO pledges(amount) VALUES (?);",
      params = list(amt)
    )
  })

  # Status shown in the UI
  output$status <- renderPrint({
    n <- dbGetQuery(conn, "SELECT COUNT(*) AS n FROM pledges;")$n[1]
    cat("Pledges in DB:", n, "\n")
  })

  # FD + pledge monitor: logs every 5 seconds
  observe({
    invalidateLater(5000, session)

    # Count FDs
    files <- list.files("/proc/self/fd", full.names = TRUE)
    fd_count <- length(files)

    # Count pledges
    n <- tryCatch(
      dbGetQuery(conn, "SELECT COUNT(*) AS n FROM pledges;")$n[1],
      error = function(e) NA_integer_
    )

    # Log to server output
    message(sprintf("MWE: Open FD count = %s, pledges = %s", fd_count, n))
  })
}

shinyApp(ui, server)
