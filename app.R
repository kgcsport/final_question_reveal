# app.R
library(shiny)
library(future)
library(promises)

## ---- Future plan (for async) ----
# On Posit Connect/Cloud, multisession is fine.
future::plan(multisession)

## ---- Simple backup target: a text file ----
BACKUP_FILE <- file.path(tempdir(), "backup_log.txt")

log_msg <- function(...) {
  msg <- paste(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "-", paste(..., collapse = " "))
  cat(msg, "\n")
}

backup_to_file <- function(label = "backup") {
  log_msg("backup_to_file(): starting", sprintf("(%s)", label))

  line <- paste(
    format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    "- BACKUP -",
    label
  )

  # Append a line to the backup file
  cat(line, "\n", file = BACKUP_FILE, append = TRUE)

  log_msg("backup_to_file(): done", sprintf("(%s)", label))
  invisible(TRUE)
}

## ---- Async wrapper using future + promises ----
backup_async <- function(label = "async backup") {
  log_msg("backup_async(): scheduling", sprintf("(%s)", label))

  future_promise({
    backup_to_file(label)
  }) %...>% (function(res) {
    log_msg("backup_async(): resolved", sprintf("(%s, result=%s)", label, as.character(res)))
  }) %...!% (function(e) {
    log_msg("backup_async(): ERROR", sprintf("(%s) %s", label, conditionMessage(e)))
  }) -> .ignored

  invisible(TRUE)
}

## ---- UI ----
ui <- fluidPage(
  titlePanel("MWE: Async backup + onStop backup"),

  fluidRow(
    column(
      width = 6,
      h4("Actions"),
      actionButton("do_work", "Do some work (debounced backup)"),
      br(), br(),
      actionButton("backup_now", "Backup now (async)"),
      br(), br(),
      strong("Backup file path:"),
      verbatimTextOutput("backup_path"),
      strong("Last backup trigger:"),
      verbatimTextOutput("last_backup_trigger")
    ),
    column(
      width = 6,
      h4("Instructions"),
      tags$ol(
        tags$li("Click 'Do some work' a few times quickly; see debounced async backups."),
        tags$li("Click 'Backup now' to run a manual async backup."),
        tags$li("Check the backup log file (path shown on left)."),
        tags$li("Close the app / let it idle, then restart and inspect the file to see an onStop backup line.")
      )
    )
  )
)

## ---- Server ----
server <- function(input, output, session) {
  # Track when we last triggered a backup (for throttling)
  last_backup_ts <- reactiveVal(Sys.time() - 3600)

  output$backup_path <- renderText(BACKUP_FILE)
  output$last_backup_trigger <- renderText({
    as.character(last_backup_ts())
  })

  ## ---- Debounced backup trigger ----
  backup_trigger <- reactiveVal(NULL)
  backup_trigger_debounced <- debounce(backup_trigger, 2000)  # 2 second quiet period

  # Clicking "Do some work" just updates the trigger
  observeEvent(input$do_work, {
    log_msg("do_work clicked")
    backup_trigger(Sys.time())
  })

  # When the debounced trigger fires, run async backup at most once per 10 seconds
  observeEvent(backup_trigger_debounced(), {
    now <- Sys.time()
    if (difftime(now, last_backup_ts(), units = "secs") < 10) {
      log_msg("Debounced backup skipped (too soon since last backup).")
      return()
    }

    last_backup_ts(now)
    log_msg("Debounced backup starting (async)...")
    backup_async("debounced backup")
  })

  ## ---- Manual "Backup now" button (async) ----
  observeEvent(input$backup_now, {
    log_msg("Manual backup button clicked.")
    showNotification("Manual backup started (async)...", type = "message")
    backup_async("manual backup")
  })

  ## ---- onStop: synchronous backup on app shutdown ----
  onStop(function() {
    log_msg("onStop(): application is stopping; running shutdown backup (sync).")
    try({
      backup_to_file("shutdown backup")
      log_msg("onStop(): shutdown backup complete.")
    }, silent = TRUE)
  })
}

shinyApp(ui, server)
