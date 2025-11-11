# ============================================================
# Public Econ "Final Question Game" — SQLite-persistent (hardened)
# ============================================================

if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
pacman::p_load(shiny, DT, bcrypt, tidyverse, DBI, RSQLite, pool, base64enc, glue)

`%||%` <- function(a, b) if (!is.null(a) && !is.na(a) && nzchar(as.character(a))) a else b

options(shiny.sanitize.errors = FALSE)
options(shiny.fullstacktrace = TRUE)

# Log helper: writes to stderr (shows in Connect logs)
logf <- function(...) cat(format(Sys.time()), "-", paste(..., collapse=" "), "\n", file=stderr())

logf("getwd:", getwd())

env_vars <- c("CRED_B64", "CRED_PATH", "CRED_CSV")
vals <- Sys.getenv(env_vars, unset = "")
logf("env present:", paste(env_vars, nzchar(vals), sep="=", collapse="; "))

# For debugging only: log lengths, not contents
logf("CRED_B64 nchar:", nchar(Sys.getenv("CRED_B64", "")))
logf("CRED_CSV nchar:", nchar(Sys.getenv("CRED_CSV", "")))
logf("CRED_PATH:", Sys.getenv("CRED_PATH", ""))

get_credentials <- function() {
  b64 <- Sys.getenv("CRED_B64", "")
  if (nzchar(b64)) {
    logf("Loading credentials from CRED_B64")
    raw <- base64decode(b64)
    return(read_csv(raw, show_col_types = FALSE, trim_ws = TRUE))
  }
  csv <- Sys.getenv("CRED_CSV", "")
  if (nzchar(csv)) {
    logf("Loading credentials from CRED_CSV")
    con <- textConnection(csv); on.exit(close(con))
    return(read_csv(con, show_col_types = FALSE, trim_ws = TRUE))
  }
  path <- Sys.getenv("CRED_PATH", "")
  if (nzchar(path)) {
    logf("Loading credentials from CRED_PATH:", path)
    stopifnot(file.exists(path))
    return(read_csv(path, show_col_types = FALSE, trim_ws = TRUE))
  }
  stop("No credentials found: set CRED_B64 (preferred), or CRED_CSV, or CRED_PATH")
}

CRED <- tryCatch(
  get_credentials(),
  error = function(e) { logf("Credential load error:", conditionMessage(e)); stop(e) }
)

# -------------------------
# Questions (unchanged)
# -------------------------
QUESTIONS <- list(
  HTML("<b>Public Goods & Private Goods</b><br> True, False, or Uncertain: The key difference between <i>public goods</i> and <i>private goods</i> is that all consumers have to pay the same price for public goods, but firms can charge different prices for private goods."),
  HTML("<b>Actuarial Fairness</b><br> True, False, or Uncertain: If the price of insurance is higher than actuarially fair, individuals should not buy insurance."),
  HTML("<b>Horizontal Equity</b><br> True, false or uncertain: Exempting tips from income taxes reduces horizontal equity in the tax system."),
  HTML("<b>Moral Hazard</b><br> Empirical research shows that the duration of unemployment increases as unemployment insurance benefits become more generous. True, false or uncertain: This increase reflects a moral hazard problem."),
  HTML("<b>Capital Income Tax and Risk</b><br> True, false or uncertain: a capital income tax is a distortion to risk taking."),
  HTML("<b>Tiebout Choice</b><br> True, False, or Uncertain: The Tiebout model implies that local public goods can be provided efficiently without government intervention. Explain briefly."),
  HTML("<b>Externalities and Targeting</b><br> The class reading Bento (2014) analyzed a policy that gave HOV lane access to hybrid and electric vehicles with a single occupant. This policy was touted as a free way to reduce air pollution. Explain why this policy is not free."),
  HTML("<b>Carbon Tax or Solar Panel Subsidies</b><br> If you were advising Governor Hochul on how to address climate change, would you recommend a carbon tax or subsidies for solar panel production? Provide one reason your recommendation is better than the other option."),
  HTML("<b>Excess Burden</b><br> When energy prices rise, people do not change their energy use (at least in the short term). On the other hand, when prices of movie tickets go up people go to see movies much less often. A policymaker argues that a tax on energy is therefore more damaging than a tax on movie tickets, because people can mitigate their behavior to reduce their utility loss from the latter. Hence, it is better to tax movie tickets than to tax energy. Do you agree? Explain."),
  HTML("<b>Tiebout and housing markets</b><br> True, False, or Uncertain: In the Tiebout model, local property taxes serve as a non-distortionary “price” for public goods, so communities with higher demand for public spending will have higher property values and taxes in equilibrium. Explain the link between housing prices, taxes, and sorting.")
)

title_from_html <- function(x) {
  s <- as.character(x)
  t <- stringr::str_extract(s, "(?s)(?<=<b>).+?(?=</b>)")
  if (is.na(t)) {
    t <- stringr::str_remove_all(s, "<[^>]+>")
    t <- stringr::str_squish(t)
    t <- stringr::str_trunc(t, 120)
  }
  t
}

# -------------------------
# SQLite connection + schema (HARDENED)
# -------------------------

safe_data_dir <- function() {
  d <- Sys.getenv("CONNECT_CONTENT_DIR", "")
  if (nzchar(d)) {
    d <- file.path(d, "data")
  } else {
    d <- tryCatch(tools::R_user_dir("finalquestion", "data"), error = function(e) "")
    if (!nzchar(d)) d <- file.path(tempdir(), "finalquestion")
  }
  if (!dir.exists(d)) dir.create(d, recursive = TRUE, showWarnings = FALSE)
  d
}

# --- DB helpers (DRY all DBI:: calls) ---

get_con <- local({
  cached <- NULL
  function() {
    # reuse cached if alive
    if (inherits(cached, "pool") && !pool::poolClosed(cached)) return(cached)
    # reuse global db if alive (don’t overwrite it)
    if (exists("db", inherits = TRUE) &&
        inherits(db, "pool") && !pool::poolClosed(db)) {
      cached <<- db
      # logf("Reusing global DB pool.")
      return(cached)
    }
    # ONLY here do we create a fresh pool (nothing alive)
    # logf("Reopening DB pool...")
    cached <<- pool::dbPool(RSQLite::SQLite(), dbname = db_path)
    cached
  }
})

# Default to global pool unless a connection/pool is explicitly passed
db_exec <- function(sql, params = NULL, con = NULL) {
  conn <- if (is.null(con)) get_con() else con
  DBI::dbExecute(conn, sql, params = params)
}
db_query <- function(sql, params = NULL, con = NULL) {
  conn <- if (is.null(con)) get_con() else con
  DBI::dbGetQuery(conn, sql, params = params)
}

touch_heartbeat <- function() db_exec("UPDATE game_state SET updated_at = CURRENT_TIMESTAMP WHERE id=1;")
is_open         <- function(x) isTRUE(as.logical(x))

# Common selects
q_settings   <- function() db_query("SELECT * FROM settings WHERE id=1;")
q_state      <- function() db_query("SELECT * FROM game_state WHERE id=1;")
q_round_tot  <- function(r) db_query("SELECT COALESCE(SUM(pledge),0) AS pledged,
                                      COUNT(DISTINCT user_id) AS n_users
                                      FROM pledges WHERE round=?;", list(as.integer(r)))
q_user_round <- function(uid, r) db_query("SELECT COALESCE(pledge,0) AS p
                                           FROM pledges WHERE user_id=? AND round=?;",
                                           list(uid, as.integer(r)))

logf("Setting up database connection...")
db_path <- file.path(safe_data_dir(), "appdata.sqlite")
db <- pool::dbPool(RSQLite::SQLite(), dbname = db_path)

# FIX: reliability PRAGMAs
try({
  db_exec("PRAGMA journal_mode=WAL;")
  db_exec("PRAGMA synchronous=NORMAL;")
  db_exec("PRAGMA busy_timeout=5000;")
}, silent = TRUE)

# Close pool on session end & process exit
reg.finalizer(environment(), function(e) try(pool::poolClose(db), silent = TRUE), onexit = TRUE)

logf("Initializing database...")
init_db <- function() {
  logf("Creating users table...")
  db_exec("
    CREATE TABLE IF NOT EXISTS users (
      user_id TEXT PRIMARY KEY,
      display_name TEXT,
      is_admin INTEGER DEFAULT 0
    );")
  db_exec("
    CREATE TABLE IF NOT EXISTS settings (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      cost REAL,
      max_per_student REAL,
      slider_step REAL,
      max_for_admin REAL,
      round_timeout_sec REAL,
      shortfall_policy TEXT
    );")
  db_exec("
    CREATE TABLE IF NOT EXISTS game_state (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      round INTEGER,
      round_open INTEGER,
      carryover REAL,
      unlocked_units INTEGER,
      scale_factor REAL,
      question_text TEXT,
      started_at TEXT,
      updated_at TEXT DEFAULT (CURRENT_TIMESTAMP)
    );")
  db_exec("
    CREATE TABLE IF NOT EXISTS pledges (
      user_id TEXT,
      round INTEGER,
      pledge REAL,
      charged REAL DEFAULT 0,
      when_ts TEXT DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (user_id, round)
    );")
  db_exec("CREATE INDEX IF NOT EXISTS ix_pledges_round ON pledges(round);")
  db_exec("
    CREATE TABLE IF NOT EXISTS charges (
      user_id TEXT,
      round INTEGER,
      amount REAL,
      charged_at TEXT DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (user_id, round)
    );")
  
  # Seed settings/state if missing
  nset <- db_query("SELECT COUNT(*) n FROM settings WHERE id=1;")$n[1]
  if (is.na(nset) || nset == 0) {
    db_exec("
      INSERT INTO settings(id, cost, max_per_student, slider_step, max_for_admin, round_timeout_sec, shortfall_policy)
      VALUES(1, 24, 7.5, 0.5, 100, NULL, 'bank_all');")
  }
  ngs <- db_query("SELECT COUNT(*) n FROM game_state WHERE id=1;")$n[1]
  if (is.na(ngs) || ngs == 0) {
    db_exec(
     "INSERT INTO game_state(id, round, round_open, carryover, unlocked_units, scale_factor, question_text, started_at)
       VALUES(1, 1, 0, 0, 0, NULL, ?, NULL);",
      params = list(as.character(QUESTIONS[[1]]))
    )
  }

  # Upsert users from CRED
  purrr::walk(seq_len(nrow(CRED)), function(i){
    db_exec("
      INSERT INTO users(user_id, display_name, is_admin)
      VALUES(?, ?, ?)
      ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name, is_admin=excluded.is_admin;",
      params = list(CRED$user[i], CRED$name[i], as.integer(isTRUE(CRED$is_admin[i]))))
  })
}
init_db()
logf("Database initialized.")

# Convenience getters/setters with SAFE defaults
blank_settings <- function() tibble(
  id=1, cost=24, max_per_student=7.5, slider_step=0.5,
   max_for_admin=100, round_timeout_sec=NA_real_, shortfall_policy="bank_all"
)

blank_state <- function() tibble(
  id=1, round=1, round_open=0, carryover=0, unlocked_units=0,
  scale_factor=NA_real_, question_text=as.character(QUESTIONS[[1]]),
  started_at=NA_character_, updated_at=as.character(Sys.time())
)

get_settings <- function() { out <- q_settings(); if (nrow(out)) out else blank_settings() }
get_state    <- function() { out <- q_state();    if (nrow(out)) out else blank_state() }

set_settings <- function(...) {
  dots <- list(...)
  if (!length(dots)) return(invisible(TRUE))

  clauses <- c(); params <- list()
  for (nm in names(dots)) {
    val <- dots[[nm]]
    if (length(val) != 1) stop(sprintf("`%s` must be length 1", nm))
    if (is.na(val)) {
      clauses <- c(clauses, sprintf("%s = NULL", nm))
    } else {
      clauses <- c(clauses, sprintf("%s = ?", nm)); params <- c(params, list(val))
    }
  }
  db_exec(paste0("UPDATE settings SET ", paste(clauses, collapse=", "), " WHERE id=1;"), params)
  touch_heartbeat()
  invisible(TRUE)
}

set_state <- function(...) {
  dots <- list(...)
  if (!length(dots)) return(invisible(touch_heartbeat()))
  nm <- names(dots)
  sql <- paste0("UPDATE game_state SET ", paste0(nm, " = ?", collapse=", "),
                ", updated_at = CURRENT_TIMESTAMP WHERE id=1;")
  db_exec(sql, unname(dots))
  invisible(TRUE)
}

upsert_pledge <- function(user_id, round, pledge, name=NULL){
  if (!is.null(name)) {
    db_exec("UPDATE users SET display_name=? WHERE user_id=?;", params = list(name, user_id))
  }
  db_exec("
    INSERT INTO pledges(user_id, round, pledge)
    VALUES(?, ?, ?)
    ON CONFLICT(user_id, round) DO UPDATE SET pledge=excluded.pledge, when_ts=CURRENT_TIMESTAMP;",
    params = list(user_id, as.integer(round), as.numeric(pledge)))
  set_state()
}

round_totals <- function(round){
  tryCatch(
    db_query("SELECT COALESCE(SUM(pledge),0) AS pledged, COUNT(DISTINCT user_id) AS n_users
                         FROM pledges WHERE round = ?;", params = list(as.integer(round))),
    error = function(e) tibble(pledged = 0, n_users = 0)
  )
}

user_cumulative_charged <- function(user_id){
  tryCatch(
    db_query("SELECT COALESCE(SUM(amount),0) AS total FROM charges WHERE user_id=?;", params = list(user_id))$total[1],
    error = function(e) 0
  )
}

charge_round_bank_all <- function(rnd) {
  pool::poolWithTransaction(get_con(), function(con){
    db_exec("DELETE FROM charges WHERE round = ?;", params = list(as.integer(rnd)), con=con)
    db_exec("INSERT INTO charges(user_id, round, amount)
       SELECT user_id, round, pledge FROM pledges WHERE round = ?;", params = list(as.integer(rnd)), con=con)
  })
}

.compute_wtp <- function(st, s) {
  # pledged this round from DB
  rt <- db_query("SELECT COALESCE(SUM(pledge),0) AS pledged
                  FROM pledges WHERE round = ?;", params = list(as.integer(st$round)))
  pledged <- rt$pledged[1]
  eff <- pledged + st$carryover
  units <- as.integer(floor(eff / s$cost))
  list(
    pledged = pledged,
    carryover = st$carryover,
    cost = s$cost,
    effective_total = eff,
    units_now = units,
    carryforward = max(0, eff - units * s$cost)
  )
}


refund_carryover_proportionally <- function(carry){
  if (carry <= 0) return(invisible(FALSE))
  tc <- db_query("SELECT user_id, COALESCE(SUM(amount),0) AS charged FROM charges GROUP BY user_id;")
  tot <- sum(tc$charged)
  if (tot <= 0) return(invisible(FALSE))
  tc$refund <- carry * (tc$charged / tot)
  now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  pool::poolWithTransaction(get_con(), function(con){
    purrr::pwalk(tc[, c("user_id","refund")], function(user_id, refund){
      db_exec("INSERT INTO charges(user_id, round, amount, charged_at) VALUES(?, 9999, ?, ?);", params = list(user_id, -as.numeric(refund), now), con=con)
    })
    # set_state() writes outside the transaction; update via 'con' here:
    db_exec("UPDATE game_state SET carryover=0, round_open=0, updated_at=CURRENT_TIMESTAMP WHERE id=1;", con=con)
  })
  TRUE
}

render_unlocked_questions <- function(units) {
  idx <- seq_len(min(units, length(QUESTIONS)))
  tagList(
    h5(if (units == 1) "Unlocked Question" else sprintf("Unlocked Questions (%d)", units)),
    lapply(rev(idx), function(i) {
      wellPanel(div(style="font-size:1.15em; line-height:1.5;", HTML(as.character(QUESTIONS[[i]]))))
    })
  )
}

# -------------------------
# UI (same)
# -------------------------
login_ui <- function(msg = NULL) {
  fluidPage(
    titlePanel("Public Econ — Login"),
    if (!is.null(msg)) div(style="color:#b00020; font-weight:bold;", msg),
    textInput("login_user", "Username"),
    passwordInput("login_pw", "Password"),
    actionButton("login_btn", "Sign in", class="btn-primary"),
    tags$small("Use the username and password provided by your instructor.")
  )
}

admin_pledges_upload_ui <- wellPanel(
  h5("Upload prior pledges (CSV)"),
  fileInput("upload_pledges", "Choose pledges.csv", accept = ".csv"),
  uiOutput("upload_pledges_status")
)

ui <- fluidPage(
  uiOutput("auth_gate"),
  conditionalPanel("output.authed",
    tabsetPanel(
      tabPanel("Student View", uiOutput("student_ui")),
      tabPanel("Projector",    uiOutput("projector_ui")),
      tabPanel("Admin View",   uiOutput("admin_ui"))
    )
  )
)

# -------------------------
# Server
# -------------------------
logf("Starting server...")
server <- function(input, output, session) {

  rv <- reactiveValues(
    authed = FALSE,
    user = NULL,
    display = NULL,
    is_admin = FALSE,
    my_submission_pulse = 0L,
    admin_pulse = 0L,
    round_pulse = 0L
  )
  bump_admin <- function() rv$admin_pulse <<- rv$admin_pulse + 1L
  bump_round <- function() rv$round_pulse <<- rv$round_pulse + 1L

  output$authed <- reactive(rv$authed)
  outputOptions(output, "authed", suspendWhenHidden = FALSE)

  output$auth_gate <- renderUI({ if (!rv$authed) login_ui() else NULL })

  # ---- Login
  observeEvent(input$login_btn, {
    u <- trimws(input$login_user); p <- input$login_pw
    row <- CRED[CRED$user == u, , drop = FALSE]
    ok <- nrow(row) == 1 && bcrypt::checkpw(p, row$pw_hash)
    if (ok) {
      rv$authed   <- TRUE
      rv$user     <- row$user
      rv$display  <- row$name
      rv$is_admin <- isTRUE(row$is_admin[1])
    } else {
      showNotification("Login failed.", type="error")
    }
  })
  authed   <- reactive(rv$authed)
  user_id  <- reactive(rv$user)
  dispname <- reactive(rv$display)
  is_admin <- reactive(rv$is_admin)

  # ---- reactivePoll heartbeats (HARDENED)
  settings_poll <- reactivePoll(
    2000, session,
    checkFunc = function() {
      out <- try(db_query("SELECT updated_at FROM game_state WHERE id=1;"), silent = TRUE)
      if (inherits(out, "try-error") || !is.data.frame(out) || nrow(out) == 0) {
        as.character(Sys.time())  # force a tick; we'll serve defaults below
      } else {
        out$updated_at[1] %||% as.character(Sys.time())
      }
    },
    valueFunc = function() {
      s  <- get_settings()
      st <- get_state()
      list(settings = s, state = st)
    }
  )

  current_settings <- reactive(settings_poll()$settings)
  current_state    <- reactive(settings_poll()$state)

  # ---- Helpers
  remaining_cap_for <- function(uid) {
    s  <- get_settings()
    st <- get_state()
    spent <- tryCatch(db_query("SELECT COALESCE(SUM(amount),0) AS total FROM charges WHERE user_id=?;",
                              list(uid))$total[1], error=function(e) 0)
    curp  <- tryCatch(q_user_round(uid, st$round)$p[1], error=function(e) 0)
    curp  <- ifelse(is.na(curp), 0, curp)
    pmax(0, as.numeric(s$max_per_student - spent - curp))
  }

  # snap to slider step (and coerce nonnegative)
  snap_to_step <- function(x, step) {
    x <- suppressWarnings(as.numeric(x))
    if (!is.finite(x) || x < 0) return(0)
    round(x / step) * step
  }

  output$whoami <- renderUI({
    req(authed())
    HTML(sprintf("<b>Logged in as:</b> %s (%s)", user_id(), dispname()))
  })

  # ---- Student UI
  student_ui <- fluidPage(
    h4(textOutput("round_title")),
    uiOutput("game_rules"),
    tags$hr(),
    uiOutput("round_status"),
    tags$hr(),
    uiOutput("whoami"),
    tags$hr(),
    uiOutput("pledge_ui"),
    actionButton("submit_pledge", "Submit pledge", class = "btn-primary"),
    tags$small(" You can update your pledge any time while the round is open."),
    tags$hr(),
    h5("Round progress"),
    uiOutput("progress_text"),
    tags$hr(),
    h5("Your submissions"),
    DTOutput("my_submissions"),
    DTOutput("my_history_table")
  )
  output$student_ui <- renderUI({ req(authed()); student_ui })

  output$round_title <- renderText({
    s <- current_settings(); st <- current_state()
    validate(need(nrow(s) == 1 && nrow(st) == 1, "Loading…"))
    sprintf("Round %g — Cost to unlock: %g points", st$round, s$cost)
  })

  output$game_rules <- renderUI({
    s <- current_settings()
    HTML(sprintf(
      "<ol style='margin-left: 1.2em; line-height: 1.4;'>
        <li>Each student starts with <b>%g points</b> they can pledge toward unlocking potential final exam questions.</li>
        <li>Each potential question costs <b>%g total points</b>. The class is paying collectively. If the class can afford one or more questions in a round (including leftover points carried in from previous rounds), those questions are unlocked.</li>
        <li>The first question unlocked is 100 percent likely to appear on the exam. The Qth additional question will appear with probability 1/Q where Q is the number of unlocked questions so far.</li>
        <li>You are only actually charged for the share of your pledge that is needed to buy the question(s). If the class over-pledges in a round, everyone is scaled down proportionally so that nobody overpays.</li>
        <li>Any extra points that were pledged but not needed roll forward as carryover to help fund the next round.</li>
        <li>Your pledge limit shrinks after you spend. In later rounds you can only pledge points you have not already pledged in earlier rounds.</li>
        <li>If, at the end of a round, the class doesn’t have enough (carryover + new pledges) to afford even one new question, nobody is charged for that round and no additional question is unlocked.</li>
      </ol>",
      s$max_per_student, s$cost
    ))
  })

  output$round_status <- renderUI({
    st <- current_state()
    if (is_open(st$round_open)) {
      tagList(
        p(sprintf("Pledging is OPEN. Submit your pledge (0–%g).", current_settings()$max_per_student)),
        wellPanel(strong("This round's question is hidden until funded."),
                  p("Once the class total meets the cost, the question will appear here."))
      )
    } else if (st$unlocked_units > 0) {
      render_unlocked_questions(st$unlocked_units)
    } else {
      p("Pledging is CLOSED. Waiting for instructor to open the next round.")
    }
  })

  output$progress_text <- renderUI({
    st <- current_state()
    HTML(paste(
      sprintf("Round: %g | Round open: %s", st$round, as.logical(is_open(st$round_open))),
      sprintf("Questions unlocked (total): %s", st$unlocked_units),
      sep = "<br/>"
    ))
  })

  # ---- Pledge UI
  output$pledge_ui <- renderUI({
    st <- current_state(); s <- current_settings()
    cap <- if (is_admin()) s$max_for_admin else remaining_cap_for(user_id())
    cur <- isolate(if (is.null(input$pledge)) 0 else as.numeric(input$pledge))
    val <- min(cur, cap)
    tagList(
      sliderInput("pledge", "Your pledge for THIS round:", min=0, max=cap, value=val, step=s$slider_step),
      tags$small(if (is_admin()) {
        "You are an admin, so you can pledge any amount (up to admin cap)."
      } else if (st$round == 1) {
        sprintf("This is your first round. You have %g points to spend.", s$max_per_student)
      } else if (cap == 0) {
        "You have pledged all your points."
      } else {
        sprintf("Cap this round: %g.", cap)
      })
    )
  })

  # ---- Submit pledge
  observeEvent(input$submit_pledge, {
    req(authed())
    st <- current_state()
    if (!is_open(st$round_open)) {
      showNotification("Pledging is currently closed.", type="warning")
      return()
    }
    s <- current_settings()
    pledge <- as.numeric(input$pledge %||% 0)
    pledge <- if (is_admin()) pmin(pmax(0, pledge), s$max_for_admin) else pmin(pmax(0, pledge), s$max_per_student)
    upsert_pledge(user_id(), st$round, pledge, dispname())
    rv$my_submission_pulse <- rv$my_submission_pulse + 1L
    showNotification(glue("Pledge of {pledge} points submitted successfully."), type="message")
    bump_admin()
  })

  # ---- My submissions + history
  output$my_submissions <- renderDT({
    req(authed())

    df <- tryCatch(
      db_query(
        "SELECT round, pledge, charged, when_ts AS 'when'
        FROM pledges WHERE user_id=? ORDER BY round DESC;",
        params = list(user_id())
      ),
      error = function(e) tibble::tibble()
    ) |>
    tibble::as_tibble() |>
    dplyr::mutate(name = dispname()) |>
    dplyr::select(dplyr::any_of(c("round","name","pledge","charged","when")))

    DT::datatable(df, options = list(pageLength = 5), rownames = FALSE)
  })

  output$my_history_table <- DT::renderDT({
    req(authed())
    if (rv$my_submission_pulse == 0) {
      return(DT::datatable(data.frame(), options = list(dom = 't'), rownames = FALSE))
    }
    df <- tryCatch(
      db_query(
        "SELECT round AS Round, pledge AS Pledge, charged AS Charged, when_ts AS Submitted
         FROM pledges WHERE user_id=? ORDER BY round ASC;",
        params = list(user_id())
      ),
      error = function(e) data.frame()
    )
    if (!nrow(df)) {
      return(DT::datatable(data.frame(), options = list(dom = 't'), rownames = FALSE))
    }
    DT::datatable(df, rownames = FALSE,
                  options = list(pageLength = 10, searching = FALSE, lengthChange = FALSE, ordering = TRUE))
  })

  # -------------------------
  # Admin UI + controls
  # -------------------------
  fallback <- function(x, default) if (is.null(x) || !length(x) || (is.na(x)[1])) default else x

  settings_panel <- reactive({
    req(authed(), is_admin())
    s <- current_settings()
    wellPanel(
      h5("Game Settings"),
      fluidRow(
        column(4,
          numericInput("cfg_COST", "Cost per question", value=fallback(s$cost, 24), min=1, step=1),
          numericInput("cfg_MAX_PER_STUDENT", "Max points per student (round 1)", value=fallback(s$max_per_student, 7.5), min=0, step=0.5)
        ),
        column(4,
          numericInput("cfg_SLIDER_STEP", "Slider step size", value=fallback(s$slider_step, 0.5), min=0.1, step=0.1),
          numericInput("cfg_MAX_FOR_ADMIN", "Max pledge for admin", value=fallback(s$max_for_admin, 100), min=0, step=1)
        ),
        column(4,
        numericInput("cfg_ROUND_TIMEOUT_SEC", "Round timeout (sec, NA = off)", value = fallback(s$round_timeout_sec, NA), min=1, step=1),
          selectInput("cfg_SHORTFALL_POLICY", "Shortfall policy", choices=c("bank_all","nocharge"), selected=fallback(s$shortfall_policy, "bank_all"))
        )
      ),
      actionButton("apply_settings", "Apply settings", class="btn-secondary")
    )
  })

  admin_controls <- reactive({
    req(authed())
    if (!isTRUE(is_admin())) return(fluidPage(h4("Admin View"), p("You must be an instructor to view this page.")))
    st <- current_state()
    fluidPage(
      h4("Instructor Controls"),
      tags$hr(),
      settings_panel(),
      tags$hr(),
      h5("Round content"),
      selectInput("round_selector", sprintf("Load preset (1–%d):", length(QUESTIONS)), choices = 1:length(QUESTIONS),
                  selected = st$round, width = "180px"),
      tags$small("Select a preset to auto-fill the question; you can edit below."),
      tags$hr(),
      fluidRow(
        column(3, actionButton("open_round",  "Open pledging", class = "btn-success")),
        column(3, actionButton("close_round", "Close pledging now", class = "btn-danger")),
        column(3, actionButton("next_round",  "Next round", class = "btn-primary")),
        column(3, actionButton("previous_round",  "Previous round", class = "btn-primary")),
        column(3, actionButton("reset_all",   "RESET all (keep roster)", class = "btn-outline-danger")),
        column(3, actionButton("end_game", "End game & redistribute", class="btn-warning"))
      ),
      wellPanel(
        h5(textOutput("admin_round_title")),
        verbatimTextOutput("admin_round_status")
      ),
      admin_pledges_upload_ui,
      h5("Pledges this round"),
      wellPanel(
        h5("Adjust / pledge for a student"),
        fluidRow(
          column(4,
            selectInput("admin_target_user", "Student",
              choices = {
                us <- db_query("SELECT user_id, display_name FROM users WHERE is_admin=0 ORDER BY display_name;")
                setNames(us$user_id, us$display_name)
              }
            ),
            htmlOutput("admin_target_info")   # remaining cap etc.
          ),
          column(4,
            numericInput("admin_amount", "Amount",
              value = 0, min = 0, step = current_settings()$slider_step)
          ),
          column(4, br(),
            actionButton("admin_do_pledge", "Pledge for student", class = "btn-primary"),
            tags$span(" "),
            actionButton("admin_do_add",    "Add points", class = "btn-success")
          )
        )
      ),
      tags$hr(),
      textAreaInput("admin_question", "Question (revealed only if cost is met):",
                    value = title_from_html(QUESTIONS[[min(current_state()$unlocked_units + 1L, length(QUESTIONS))]]),
                    width="100%", height="140px"),
      actionButton("save_texts", "Save question", class = "btn-secondary"),
      h5("Export / Save Results"),
      fluidRow(
        column(3, downloadButton("dl_pledges_csv", "Download pledges (CSV)")),
        column(3, downloadButton("dl_roster_csv",  "Download roster (CSV)"))
      )
    )
  })
  output$admin_ui <- renderUI(admin_controls())

  observeEvent(input$apply_settings, {
    req(is_admin())

    # Treat blank as NA so we clear the timeout (set SQL NULL)
    rt <- input$cfg_ROUND_TIMEOUT_SEC
    if (is.character(rt) && !nzchar(rt)) rt <- NA_real_
    if (length(rt) == 0) rt <- NA_real_

    set_settings(
      cost              = input$cfg_COST,
      max_per_student   = input$cfg_MAX_PER_STUDENT,
      slider_step       = input$cfg_SLIDER_STEP,
      max_for_admin     = input$cfg_MAX_FOR_ADMIN,
      round_timeout_sec = rt,                     # NA clears, number sets, NULL would keep
      shortfall_policy  = input$cfg_SHORTFALL_POLICY
    )

    showNotification("Settings updated for all players.", type = "message")
    bump_admin()
  })

  output$admin_round_title <- renderText({
    st <- current_state(); s <- current_settings()
    sprintf("Round %g — Cost: %g points", st$round, s$cost)
  })

  admin_hdr <- eventReactive(rv$admin_pulse, {
    st <- current_state()
    s  <- current_settings()
    rt <- round_totals(st$round)
    list(round_open = is_open(st$round_open), unlocked = st$unlocked_units, sum_pledge = rt$pledged, scale = st$scale_factor,
    n_submit = rt$n_users, n_students = sum(!as.logical(CRED$is_admin)))
  }, ignoreInit = FALSE)

  output$admin_round_status <- renderText({
    h <- admin_hdr()
    paste(
      sprintf("Round open: %s | Questions unlocked (total): %s", as.logical(h$round_open), h$unlocked),
      sprintf("Submissions this round: %d / %d students", h$n_submit, h$n_students),
      if (!is.na(h$scale)) sprintf("Scale factor this round: %.3f", h$scale) else "Scale factor: (n/a)",
      sep = "\n"
    )
  })

  # live display of remaining cap & current pledge for selected student
  output$admin_target_info <- renderUI({
    req(is_admin(), input$admin_target_user)
    s  <- current_settings()
    st <- current_state()
    uid <- input$admin_target_user

    cap <- remaining_cap_for(uid)

    curp <- tryCatch(
      db_query(
        "SELECT COALESCE(pledge,0) AS p FROM pledges WHERE user_id=? AND round=?;",
        params = list(uid, st$round))$p[1],
      error = function(e) 0
    ); curp <- ifelse(is.na(curp), 0, curp)

    HTML(sprintf(
      "<div>
        <b>Round:</b> %d<br/>
        <b>Remaining cap:</b> %.2f (cannot pledge beyond this)<br/>
        <b>Current pledge this round:</b> %.2f<br/>
        <b>Step size:</b> %.2f
      </div>",
      st$round, cap, curp, s$slider_step
    ))
  })

  # sanitize admin_amount on change: snap to step and not exceed remaining cap
  observeEvent(list(input$admin_amount, input$admin_target_user), {
    req(is_admin(), input$admin_target_user)
    s <- current_settings()
    step <- s$slider_step
    uid  <- input$admin_target_user
    cap  <- remaining_cap_for(uid)

    amt  <- snap_to_step(input$admin_amount, step)
    if (is.na(amt)) amt <- 0
    if (amt > cap) amt <- cap
    if (!identical(amt, input$admin_amount)) {
      updateNumericInput(session, "admin_amount", value = amt)
    }
  }, ignoreInit = FALSE)

  # Pledge on behalf of student
  observeEvent(input$admin_do_pledge, {
    req(is_admin(), input$admin_target_user)
    s   <- current_settings()
    uid <- input$admin_target_user
    amt <- snap_to_step(input$admin_amount, s$slider_step)
    cap <- remaining_cap_for(uid)

    if (amt <= 0) {
      showNotification("Amount must be > 0.", type="warning"); return()
    }
    if (amt > cap + 1e-9) {
      showNotification("Amount exceeds remaining cap.", type="error"); return()
    }
    st <- current_state()
    upsert_pledge(uid, st$round, amt, name = NULL)
    showNotification(glue::glue("Pledged {amt} points for {uid}."), type="message")

    # heartbeat so everyone refreshes
    touch_heartbeat()
    bump_admin(); rv$my_submission_pulse <- rv$my_submission_pulse + 1L
  }, ignoreInit = TRUE)

  # Add points (increase capacity immediately via a negative 'charge' record)
  observeEvent(input$admin_do_add, {
    req(is_admin(), input$admin_target_user)
    s   <- current_settings()
    uid <- input$admin_target_user
    amt <- snap_to_step(input$admin_amount, s$slider_step)
    if (amt <= 0) {
      showNotification("Amount must be > 0.", type="warning"); return()
    }
    # Negative 'charge' adds back capacity (round 9998 reserved for admin gifts)
    db_exec(
      "INSERT INTO charges(user_id, round, amount)
      VALUES(?, 9998, ?)
      ON CONFLICT(user_id, round) DO UPDATE SET amount = amount + excluded.amount;",
      params = list(uid, -amt)
    )
    showNotification(glue::glue("Added {amt} points to {uid}."), type="message")

    db_exec("UPDATE game_state SET updated_at = CURRENT_TIMESTAMP WHERE id=1;")
    bump_admin(); bump_round()
  }, ignoreInit = TRUE)



  # ---- Projector view
  output$projector_ui <- renderUI({
    req(authed())
    st <- current_state()
    if (is_open(st$round_open) && !isTRUE(is_admin())) {
      return(
        wellPanel(
          h4("Projector hidden until the round closes."),
          p(sprintf("Round %g is currently open. You’ll see results after it closes.", st$round))
        )
      )
    }
    fluidPage(
      h4("WTP (Current Round)"),
      fluidRow(
        column(8, plotOutput("wtp_hist", height = 320)),
        column(4,
          tags$div(style="font-size: 1.1em; margin-top: 1em;", DTOutput("wtp_total")),
          tags$div(style="margin-top: 0.5em;", textOutput("wtp_stats")),
          tags$hr()
        )
      ),
      tags$hr(),
      h5("Unlocked Questions"),
      uiOutput("projector_question")
    )
  })

  wtp_summary <- reactive({
    st <- current_state()      # <-- reactive (driven by your reactivePoll)
    s  <- current_settings()   # <-- reactive
    .compute_wtp(st, s)
  })

  output$wtp_total <- DT::renderDT({
    ws <- wtp_summary()
    logf(sprintf("wtp_summary: pledged=%f, carryover=%f, effective_total=%f, cost=%f, units_now=%d, carryforward=%f", ws$pledged, ws$carryover, ws$effective_total, ws$cost, ws$units_now, ws$carryforward))
    data <- data.frame(
      Metric = c("This round pledged","Carryover","Effective total",
                 sprintf("Carryforward (if over COST = %.2f)", ws$cost)),
      Value  = sprintf("%.2f", c(ws$pledged, ws$carryover, ws$effective_total, ws$carryforward))
    )
    datatable(data, rownames = FALSE, options = list(dom='t', paging=FALSE, ordering=FALSE), colnames=c("", ""))
  })

  round_df <- reactive({s
    st <- current_state()
    tryCatch(db_query("SELECT pledge FROM pledges WHERE round = ?;", params = list(st$round)),
             error = function(e) tibble(pledge = numeric()))
  })

  output$wtp_stats <- renderText({
    df <- round_df()
    n <- nrow(df)
    avg <- if (n) mean(df$pledge) else 0
    sprintf("Participants: %g | Mean pledge: %.2f", n, avg)
  })

  output$wtp_hist <- renderPlot({
    # df <- round_df() 
    st <- current_state()
    df <- db_query("SELECT pledge, round FROM pledges WHERE round <= ?;", params = list(st$round))
    s <- current_settings()
    
    ggplot2::ggplot(df, ggplot2::aes(x = pledge,fill=as.factor(round))) +
      ggplot2::geom_histogram(binwidth = s$slider_step, boundary = 0, closed = "left",position='dodge') +
      ggplot2::scale_x_continuous(limits = c(0, s$max_per_student),
                                  breaks = seq(0, s$max_per_student, by = s$slider_step)) +
      scale_fill_brewer(palette="Set2") +
      ggplot2::labs(x = "Pledge (WTP)", y = "Count", fill="Round") +
      ggplot2::theme_bw() +
      ggplot2::theme(legend.position = if(st$round>1) "bottom" else "none")
  })

  output$projector_question <- renderUI({
    st <- current_state(); k <- length(QUESTIONS)
    if (st$unlocked_units > 0) return(render_unlocked_questions(st$unlocked_units))
    else {
      s <- current_settings()
      wellPanel(
        h4("Question(s) locked"),
        p(sprintf("Carryover from last round: %g; COST per question: %g.", st$carryover, s$cost)),
        p("Close the round to finalize purchases.")
      )
    }
  })

  # ---- Downloads
  output$dl_roster_csv <- downloadHandler(
    filename = function() "roster_players.csv",
    content = function(file) {
      rost <- CRED |> select(user, name)
      readr::write_csv(rost, file)
    }
  )
  output$dl_pledges_csv <- downloadHandler(
    filename = function() "pledges.csv",
    content = function(file) {
      df <- db_query("SELECT * FROM pledges ORDER BY round, user_id;")
      readr::write_csv(df, file)
    }
  )

  # ---- Admin actions
  observeEvent(input$round_selector, ignoreInit = TRUE, {
    req(is_admin())
    updateTextAreaInput(session, "admin_question", value = title_from_html(QUESTIONS[[as.integer(input$round_selector)]]))
  })

  observeEvent(input$save_texts, {
    req(is_admin())
    set_state(question_text = title_from_html(input$admin_question))
    showNotification("Question saved for this round.", type="message")
  })

  observeEvent(input$open_round, ignoreInit = TRUE, {
    req(is_admin())
    st <- current_state()
    set_state(round_open = 1, scale_factor = NA, started_at = as.character(Sys.time()))
    db_exec("DELETE FROM pledges WHERE round = ?;", params = list(st$round))
    showNotification(glue("Pledging is OPEN for round {st$round}. Carryover available: {st$carryover}."), type="message")
    bump_admin()
  })

  observeEvent(input$close_round, ignoreInit = TRUE, {
    req(is_admin())
    st <- get_state(); s <- get_settings()
    if (!is_open(st$round_open)) return(NULL)

    ws <- wtp_summary()

    if (identical(s$shortfall_policy, "bank_all")) {
      charge_round_bank_all(st$round)
      set_state(scale_factor = 1)
    } else {
      if (ws$units_now > 0) {
        charge_round_bank_all(st$round)
        set_state(scale_factor = 1)
      } else {
        db_exec("DELETE FROM charges WHERE round = ?;", list(st$round))
        set_state(scale_factor = NA)
      }
    }

    set_state(round_open = 0,
              carryover  = ws$carryforward,
              unlocked_units = st$unlocked_units + ws$units_now)

    showNotification(
      if (ws$units_now > 0)
        glue::glue("Purchased {ws$units_now} question(s). New carryover: {round(ws$carryforward, 2)}.")
      else if (identical(s$shortfall_policy, "bank_all"))
        glue::glue("Not funded — pledges banked. New carryover: {round(ws$carryforward, 2)}. At game over, carryover will be refunded proportionally.")
      else
        "Not funded — no one charged; no carryover added.",
      type = if (ws$units_now > 0) "message" else "warning"
    )
    touch_heartbeat(); bump_admin()
  })

  observeEvent(input$next_round, ignoreInit = TRUE, {
    req(is_admin())
    st <- current_state()
    new_round <- st$round + 1L
    set_state(round = new_round, round_open = 0, scale_factor = NA, started_at = NA)
    updateTextAreaInput(session, "admin_question", value = title_from_html(QUESTIONS[[new_round]]))
    showNotification(glue("Moved to round {new_round}. Carryover available: {current_state()$carryover}."), type="message")
    bump_admin()
  })

  observeEvent(input$previous_round, ignoreInit = TRUE, {
    req(is_admin())
    st <- current_state()
    new_round <- st$round - 1L
    set_state(round = new_round, round_open = 0, scale_factor = NA, started_at = NA)
    updateTextAreaInput(session, "admin_question", value = title_from_html(QUESTIONS[[new_round]]))
    showNotification(glue("Moved to round {new_round}. Carryover available: {current_state()$carryover}."), type="message")
    bump_admin()
  })

  observeEvent(input$reset_all, ignoreInit = TRUE, {
    req(is_admin())
    db_exec("DELETE FROM pledges;")
    db_exec("DELETE FROM charges;")
    set_state(unlocked_units = 0L, carryover = 0, round = 1, round_open = 0, scale_factor = NA,
              started_at = NA, question_text = as.character(QUESTIONS[[1]]))
    updateTextAreaInput(session, "admin_question", value = title_from_html(QUESTIONS[[1]]))
    showNotification("All rounds and pledges reset (roster kept).", type="error")
    bump_admin()
  })

  output$upload_pledges_status <- renderUI({
    if (!isTRUE(is_admin())) return(NULL)
    if (!is.null(input$upload_pledges)) tags$span(style="color:green;", paste0("Imported: ", input$upload_pledges$name))
  })

  observeEvent(input$upload_pledges, {
    req(is_admin())
    finfo <- input$upload_pledges
    if (!identical(tolower(tools::file_ext(finfo$datapath)), "csv")) {
      showNotification("Please upload a CSV file.", type = "error"); return()
    }

    pledges_df <- tryCatch(readr::read_csv(finfo$datapath, show_col_types = FALSE),
                          error = function(e) NULL)

    need_cols <- c("round","user_id","name","pledge","charged","when")
    if (is.null(pledges_df) || !all(need_cols %in% names(pledges_df))) {
      showNotification("Failed to upload: required columns missing.", type = "error"); return()
    }

    # ---- Hard coercions to base types (avoid S4/S3 surprises) ----
    # Make a shallow copy to avoid tibble subclass quirks
    pledges_df <- as.data.frame(pledges_df, stringsAsFactors = FALSE)

    # Coerce columns; suppressWarnings only around numeric parsing
    pledges_df$user_id <- as.character(pledges_df$user_id)
    pledges_df$name    <- as.character(pledges_df$name)
    pledges_df$round   <- suppressWarnings(as.integer(pledges_df$round))
    pledges_df$pledge  <- suppressWarnings(as.numeric(pledges_df$pledge))
    pledges_df$charged <- suppressWarnings(as.numeric(pledges_df$charged))

    # Normalizations
    pledges_df$name[is.na(pledges_df$name)] <- ""
    # (optional) Treat missing charged as 0
    # pledges_df$charged[is.na(pledges_df$charged)] <- 0

    # Filter obviously bad rows early
    pledges_df <- subset(pledges_df, !is.na(user_id) & nzchar(user_id) & !is.na(round))

    pool::poolWithTransaction(get_con(), function(con){

      # 1) Upsert users (character, character)
      purrr::pwalk(pledges_df[, c("user_id","name")], function(user_id, name){
        DBI::dbExecute(
          con,
          "INSERT INTO users(user_id, display_name)
          VALUES(?,?)
          ON CONFLICT(user_id) DO UPDATE SET display_name = excluded.display_name;",
          params = list(as.character(user_id), as.character(name))
        )
      })

      # 2) Upsert pledges (character, integer, numeric)
      purrr::pwalk(pledges_df[, c("user_id","round","pledge")], function(user_id, round, pledge){
        if (!is.na(round) && !is.na(pledge)) {
          DBI::dbExecute(
            con,
            "INSERT INTO pledges(user_id, round, pledge, when_ts)
            VALUES(?,?,?,CURRENT_TIMESTAMP)
            ON CONFLICT(user_id, round)
            DO UPDATE SET pledge = excluded.pledge, when_ts = CURRENT_TIMESTAMP;",
            params = list(as.character(user_id), as.integer(round), as.numeric(pledge))
          )
        }
      })

      # 3) Optional charges (character, integer, numeric)
      if (!isTRUE(all(is.na(pledges_df$charged)))) {
        purrr::pwalk(pledges_df[, c("user_id","round","charged")], function(user_id, round, charged){
          if (!is.na(charged) && charged != 0 && !is.na(round)) {
            DBI::dbExecute(
              con,
              "INSERT INTO charges(user_id, round, amount)
              VALUES(?,?,?)
              ON CONFLICT(user_id, round) DO UPDATE SET amount = excluded.amount;",
              params = list(as.character(user_id), as.integer(round), as.numeric(charged))
            )
          }
        })
      }
    })

    # Heartbeat + recompute state (reuse your helper)
    touch_heartbeat()

    ws <- .compute_wtp(st, s)
    set_state(unlocked_units = ws$units_now, carryover = ws$carryforward)

    touch_heartbeat()
    showNotification("Prior pledges uploaded and set.", type = "message")
    bump_admin(); bump_round()
  })

  observeEvent(input$end_game, ignoreInit = TRUE, {
    req(is_admin())
    st <- current_state()
    ok <- refund_carryover_proportionally(st$carryover)
    if (isTRUE(ok)) {
      showNotification("Carryover refunded proportionally.", type="message")
    } else {
      showNotification("No charges recorded; nothing to refund.", type="warning")
    }
    bump_admin()
  })
}

shinyApp(ui, server)
