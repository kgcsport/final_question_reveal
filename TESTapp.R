if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
pacman::p_load(shiny, DT, bcrypt, tidyverse)

# Load credentials once (CSV created above). Put it next to app.R or give an absolute path.
CRED <- read_csv("credentials.csv")

# Preloaded questions (NO SOLUTIONS)
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

# ===== UI HELPERS =====
logged_in_ui <- function() {
  fluidPage(
    titlePanel("Buy Potential Exam Questions (Public Goods Game)"),
    tabsetPanel(
      tabPanel("Student View", uiOutput("student_ui")),
      tabPanel("Projector",    uiOutput("projector_ui")),
      tabPanel("Admin View",   uiOutput("admin_ui"))
    )
  )
}

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

ui <- uiOutput("root_ui")

# ===== SERVER =====
server <- function(input, output, session) {

  # helper: safe defaults for admin input rendering
  fallback <- function(x, default) {
    if (is.null(x)) return(default)
    if (!length(x)) return(default)
    if (is.na(x)[1]) return(default)
    x
  }

  `%||%` <- function(x, y) if (is.null(x)) y else x

  cumulative_charged_for <- function(uid) {
    if (!nrow(G$pledges)) return(0)
    sum(G$pledges$charged[G$pledges$user_id == uid], na.rm = TRUE)
  }

  remaining_cap_for <- function(uid) {
    pmax(0, G$MAX_PER_STUDENT - cumulative_charged_for(uid))
  }


  # ---- per-session state ----
  rv <- reactiveValues(
    authed   = FALSE,
    user     = NULL,
    display  = NULL,
    is_admin = FALSE
  )

  # ---- CLASSWIDE SHARED STATE (lives across sessions) ----
  if (!exists("G", envir = .GlobalEnv)) {
    G <<- reactiveValues(

      # ==== CONFIG (editable by admin, with defaults) ====
      COST = 24,                    # cost to unlock ONE question
      MAX_PER_STUDENT = 7.5,        # per-student initial budget
      SLIDER_STEP = 0.5,            # slider granularity
      MAX_FOR_ADMIN = 100,          # instructor pledge cap
      ROUND_TIMEOUT_SEC = NA_real_, # auto-close after N seconds, NA = off
      SHORTFALL_POLICY = "bank_all",  # "bank_all" | "nocharge"

      # ==== GAME STATE ====
      round = 1,
      round_open = FALSE,
      question_unlocked = FALSE,
      started_at = NA_real_,

      # aggregates for current round
      sum_pledge = 0,
      sum_charged = 0,
      scale_factor = NA_real_,

      carryover = 0,                # leftover from previous rounds
      unlocked_units = 0L,          # total number of questions unlocked so far
      revealed_html = character(),  # HTML of unlocked questions so far (most recent first)

      # per-round "current question text" (admin may edit)
      question_text = as.character(QUESTIONS[[1]]),

      # logs
      pledges = tibble::tibble(
        round   = integer(),
        user_id = character(),
        name    = character(),
        pledge  = numeric(),
        charged = double(),
        when    = character()
      ),
      roster = CRED |> select(user, name)
    )
  }

  # keep all sessions feeling "live"
  autoInvalidate <- reactiveTimer(500, session)
  observe({
    autoInvalidate()
    invisible(G$round)
    invisible(G$round_open)
    invisible(G$question_unlocked)
    invisible(G$sum_pledge)
    invisible(G$sum_charged)
    invisible(G$scale_factor)
    invisible(G$question_text)
    invisible(G$COST)
    invisible(G$MAX_PER_STUDENT)
    invisible(G$SLIDER_STEP)
    invisible(G$carryover)
    invisible(G$unlocked_units)
  })

  # ---- auth ----
  observeEvent(input$login_btn, {
    u <- trimws(input$login_user)
    p <- input$login_pw
    row <- CRED[CRED$user == u, , drop = FALSE]

    ok <- nrow(row) == 1 && bcrypt::checkpw(p, row$pw_hash)

    if (ok) {
      rv$authed   <- TRUE
      rv$user     <- row$user
      rv$display  <- row$name
      rv$is_admin <- isTRUE(row$is_admin[1])
      # you could add roster logging here
    } else {
      # remain on login screen
    }
  })

  authed   <- reactive(rv$authed)
  user_id  <- reactive(rv$user)
  dispname <- reactive(rv$display)
  is_admin <- reactive(rv$is_admin)

  prev_round <- reactive({
    if (G$round > 1) G$round - 1 else length(QUESTIONS)
  })

  last_pledge_prev_round <- reactive({
    req(isTRUE(authed()))
    uid <- user_id()
    pr  <- prev_round()
    rows <- which(G$pledges$round == pr & G$pledges$user_id == uid)
    if (length(rows) == 0) 0 else G$pledges$pledge[rows][1]
  })

  total_charged_by_user <- function() {
    if (nrow(G$pledges) == 0) return(tibble(user_id=character(), charged=numeric()))
    G$pledges |>
      dplyr::group_by(user_id, name) |>
      dplyr::summarise(charged = sum(charged, na.rm=TRUE), .groups="drop")
  }

  # ==== DOWNLOAD HANDLERS ====
  output$dl_roster_csv <- downloadHandler(
    filename = function() "roster.csv",
    content = function(file) readr::write_csv(G$roster, file)
  )

  output$dl_pledges_csv <- downloadHandler(
    filename = function() "pledges.csv",
    content = function(file) readr::write_csv(G$pledges, file)
  )

  output$dl_all_zip <- downloadHandler(
    filename = function() {
      paste0("round_data_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".zip")
    },
    content = function(file) {
      tmpdir <- tempdir()
      pledges_path <- file.path(tmpdir, "pledges.csv")
      roster_path  <- file.path(tmpdir, "roster.csv")

      readr::write_csv(G$pledges, pledges_path)
      readr::write_csv(G$roster,  roster_path)

      oldwd <- setwd(tmpdir)
      on.exit(setwd(oldwd), add = TRUE)
      utils::zip(zipfile = file, files = c("pledges.csv", "roster.csv"))
    }
  )

  cumulative_charged_by_me <- reactive({
    uid <- user_id(); req(isTRUE(authed()))
    if (!nrow(G$pledges)) return(0)
    sum(G$pledges$charged[G$pledges$user_id == uid], na.rm=TRUE)
  })

  remaining_cap <- reactive({
    pmax(0, G$MAX_PER_STUDENT - cumulative_charged_by_me())
  })

  # ---- ROOT UI SWITCH ----
  output$root_ui <- renderUI({
    if (isTRUE(authed())) logged_in_ui() else login_ui()
  })

  output$whoami <- renderUI({
    req(isTRUE(authed()))
    HTML(sprintf("<b>Logged in as:</b> %s (%s)", user_id(), dispname()))
  })

  # ---- STUDENT UI ----
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
    DTOutput("my_submissions")
  )

  output$student_ui <- renderUI({
    req(isTRUE(authed()))
    student_ui
  })

  # helper: extract bolded title from the HTML question block
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

  # ---- ADMIN SETTINGS PANEL ----
  settings_panel <- reactive({
    req(isTRUE(authed()), isTRUE(is_admin()))

    wellPanel(
      h5("Game Settings"),
      fluidRow(
        column(
          4,
          numericInput(
            "cfg_COST",
            "Cost per question",
            value = fallback(G$COST, 24),
            min = 1, step = 1
          ),
          numericInput(
            "cfg_MAX_PER_STUDENT",
            "Max points per student (round 1)",
            value = fallback(G$MAX_PER_STUDENT, 7.5),
            min = 0, step = 0.5
          )
        ),
        column(
          4,
          numericInput(
            "cfg_SLIDER_STEP",
            "Slider step size",
            value = fallback(G$SLIDER_STEP, 0.5),
            min = 0.1, step = 0.1
          ),
          numericInput(
            "cfg_MAX_FOR_ADMIN",
            "Max pledge for admin",
            value = fallback(G$MAX_FOR_ADMIN, 100),
            min = 0, step = 1
          )
        ),
        column(
          4,
          numericInput(
            "cfg_ROUND_TIMEOUT_SEC",
            "Round timeout (sec, NA = off)",
            value = fallback(G$ROUND_TIMEOUT_SEC, NA),
            min = 1, step = 1
          ),
          selectInput(
            "cfg_SHORTFALL_POLICY",
            "Shortfall policy",
            choices = c("bank_all","nocharge"),
            selected = fallback(G$SHORTFALL_POLICY, "bank_all")
          )
        )
      ),
      actionButton("apply_settings", "Apply settings", class = "btn-secondary")
    )
  })

  # apply updated settings to G when instructor clicks
  observeEvent(input$apply_settings, {
    req(isTRUE(is_admin()))

    if (!is.null(input$cfg_COST) && !is.na(input$cfg_COST)) {
      G$COST <- as.numeric(input$cfg_COST)
    }
    if (!is.null(input$cfg_MAX_PER_STUDENT) && !is.na(input$cfg_MAX_PER_STUDENT)) {
      G$MAX_PER_STUDENT <- as.numeric(input$cfg_MAX_PER_STUDENT)
    }
    if (!is.null(input$cfg_SLIDER_STEP) && !is.na(input$cfg_SLIDER_STEP)) {
      G$SLIDER_STEP <- as.numeric(input$cfg_SLIDER_STEP)
    }
    if (!is.null(input$cfg_MAX_FOR_ADMIN) && !is.na(input$cfg_MAX_FOR_ADMIN)) {
      G$MAX_FOR_ADMIN <- as.numeric(input$cfg_MAX_FOR_ADMIN)
    }

    # timeout: NA is meaningful, allow overwrite to NA_real_
    if (!identical(input$cfg_ROUND_TIMEOUT_SEC, "")) {
      G$ROUND_TIMEOUT_SEC <- if (
        is.null(input$cfg_ROUND_TIMEOUT_SEC) ||
        is.na(input$cfg_ROUND_TIMEOUT_SEC)
      ) {
        NA_real_
      } else {
        as.numeric(input$cfg_ROUND_TIMEOUT_SEC)
      }
    }

    if (!is.null(input$cfg_SHORTFALL_POLICY) && nzchar(input$cfg_SHORTFALL_POLICY)) {
      G$SHORTFALL_POLICY <- input$cfg_SHORTFALL_POLICY
    }

    showNotification("Settings updated for all players.", type = "message")
  })

  # ---- ADMIN UI ----
  admin_controls <- reactive({
    req(isTRUE(authed()))
    if (!isTRUE(is_admin())) {
      return(fluidPage(
        h4("Admin View"),
        p("You must be an instructor to view this page.")
      ))
    }

    fluidPage(
      h4("Instructor Controls"),
      tags$hr(),

      settings_panel(),  # live config UI with defaults/fallback

      tags$hr(),
      h5("Round content"),
      selectInput(
        "round_selector",
        sprintf("Load preset (1–%d):", length(QUESTIONS)),
        choices = 1:length(QUESTIONS),
        selected = 1, width = "180px"
      ),
      tags$small("Select a preset to auto-fill the question; you can edit below."),
      tags$hr(),
      fluidRow(
        column(3, actionButton("open_round",  "Open pledging", class = "btn-success")),
        column(3, actionButton("close_round", "Close pledging now", class = "btn-danger")),
        column(3, actionButton("show_questions", "Show questions", class = "btn-primary")),
        column(3, actionButton("next_round",  "Next round", class = "btn-primary")),
        column(3, actionButton("reset_all",   "RESET all (keep roster)", class = "btn-outline-danger")),
        column(3, actionButton("end_game", "End game & redistribute", class="btn-warning"))
      ),
      h5(textOutput("admin_round_title")),
      verbatimTextOutput("admin_round_status"),
      tags$hr(),
      h5("Pledges this round"),
      tags$hr(),
      textAreaInput(
        "admin_question",
        "Question (revealed only if cost is met):",
        value = title_from_html(QUESTIONS[[1]]),
        width = "100%", height = "140px"
      ),
      actionButton("save_texts", "Save question", class = "btn-secondary"),
      tags$hr(),
      h5("Student controls (this round)"),
      fluidRow(
        column(5,
          selectizeInput(
            "admin_target_user",
            "Choose student",
            choices = CRED$user, multiple = FALSE,
            options = list(placeholder = "Start typing a username or name")
          ),
          uiOutput("admin_target_cap")
        ),
        column(4,
          numericInput(
            "admin_target_pledge", "Set pledge",
            value = 0, min = 0, step = G$SLIDER_STEP
          ),
          checkboxInput("admin_bypass_cap", "Bypass student cap", value = FALSE)
        ),
        column(3,
          actionButton("admin_set_pledge",   "Set/Update pledge", class = "btn-primary w-100"),
          tags$div(style="height:6px"),
          actionButton("admin_clear_pledge", "Clear pledge",      class = "btn-secondary w-100")
        )
      ),
      tags$small("These changes affect the selected student's pledge for the current round only."),
      tags$hr(),
      h5("Export / Save Results"),
      fluidRow(
        column(3, downloadButton("dl_pledges_csv", "Download pledges (CSV)")),
        column(3, downloadButton("dl_roster_csv",  "Download roster (CSV)")),
        column(3, downloadButton("dl_all_zip",     "Download ALL (ZIP)"))
      )
    )
  })

  output$admin_ui <- renderUI({
    admin_controls()
  })

  output$admin_target_cap <- renderUI({
    req(isTRUE(is_admin()), nzchar(input$admin_target_user))
    uid <- input$admin_target_user
    cap <- remaining_cap_for(uid)
    HTML(sprintf("<div><b>Remaining cap for %s:</b> %.2f</div>", uid, cap))
  })

  observeEvent(input$admin_set_pledge, {
    req(isTRUE(is_admin()), nzchar(input$admin_target_user))
    uid  <- input$admin_target_user
    # Try to show their roster name; fall back to uid
    nm <- G$roster$name[G$roster$user_id == uid] %||% NA_character_
    name <- if (length(nm) && !is.na(nm[1]) && nzchar(nm[1])) nm[1] else uid

    val <- as.numeric(input$admin_target_pledge %||% 0)
    if (!isTRUE(input$admin_bypass_cap)) {
      val <- pmin(val, remaining_cap_for(uid))
    }

    ridx <- which(G$pledges$round == G$round & G$pledges$user_id == uid)
    timestamp_now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

    if (length(ridx)) {
      G$pledges$pledge[ridx] <- val
      G$pledges$name[ridx]   <- name
      G$pledges$when[ridx]   <- timestamp_now
    } else {
      G$pledges <- dplyr::bind_rows(
        G$pledges,
        tibble::tibble(
          round   = G$round,
          user_id = uid,
          name    = name,
          pledge  = val,
          charged = 0,
          when    = timestamp_now
        )
      )
    }

    # Refresh this-round aggregate
    idxr <- which(G$pledges$round == G$round)
    G$sum_pledge <- if (length(idxr)) sum(G$pledges$pledge[idxr]) else 0

    showNotification(sprintf("Set %s’s pledge to %.2f for round %d.", uid, val, G$round), type="message")
  })

  observeEvent(input$admin_clear_pledge, {
    req(isTRUE(is_admin()), nzchar(input$admin_target_user))
    uid  <- input$admin_target_user
    ridx <- which(G$pledges$round == G$round & G$pledges$user_id == uid)
    if (length(ridx)) {
      G$pledges$pledge[ridx]  <- 0
      # Do not touch 'charged' here; clearing is a pre-close action.
      showNotification(sprintf("Cleared %s’s pledge for round %d.", uid, G$round), type="message")
    } else {
      showNotification("No pledge to clear for this round.", type="warning")
    }
    idxr <- which(G$pledges$round == G$round)
    G$sum_pledge <- if (length(idxr)) sum(G$pledges$pledge[idxr]) else 0
  })


  # ===== TEXT OUTPUTS / STATUS =====

  output$round_title <- renderText({
    sprintf("Round %g — Cost to unlock: %g points", G$round, G$COST)
  })

    output$game_rules <- renderUI({
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
        G$MAX_PER_STUDENT, G$COST
    ))
    })


  output$round_status <- renderUI({
    tagList(
      if (G$round_open) {
        tagList(
          p(sprintf("Pledging is OPEN. Submit your pledge (0–%g).", G$MAX_PER_STUDENT)),
          wellPanel(
            strong("This round's question is hidden until funded."),
            p("Once the class total meets the cost, the question will appear here.")
          )
        )
      } else if (G$question_unlocked) {
        tagList(
          p("Pledging is CLOSED."),
          h5("Unlocked Questions"),
            lapply(seq_along(G$revealed_html), function(i) {
            wellPanel(
              div(
                style="font-size:1.15em; line-height:1.5;",
                HTML(G$revealed_html[[i]])
              )
            )
          })
        )
      } else {
        p("Pledging is CLOSED. Waiting for instructor to open the next round.")
      },
      if (!is.na(G$scale_factor) && !G$round_open)
        p(sprintf("Scale factor applied: %.3f", G$scale_factor))
    )
  })

  output$progress_text <- renderUI({
    HTML(
    paste(
      sprintf("Round: %g | Round open: %s", G$round, G$round_open),
      sprintf("Question unlocked: %s", G$question_unlocked),
      sep = "<br/>"
    ))
  })

  # ====== PLEDGE SUBMISSION LOGIC ======

  output$pledge_ui <- renderUI({
    req(isTRUE(authed()))

    # cap logic: admins get their own cap; students get "remaining_cap"
    if (is_admin()) {
      cap <- G$MAX_FOR_ADMIN
    } else {
      cap <- remaining_cap()
    }

    current_val <- isolate(if (is.null(input$pledge)) 0 else as.numeric(input$pledge))
    val <- min(current_val, cap)

    tagList(
      sliderInput(
        "pledge", "Your pledge for THIS round:",
        min = 0, max = cap, value = val, step = G$SLIDER_STEP
      ),
      tags$small(
        if (is_admin()) {
          "You are an admin, so you can pledge any amount."
        } else if (G$round == 1) {
          sprintf("This is your first round. You have %g points to spend.", G$MAX_PER_STUDENT)
        } else if (G$round == length(QUESTIONS)) {
          sprintf("This is your last round. Last round you pledged: %g. You have %g points to spend.",
                  last_pledge_prev_round(), cap)
        } else if (cap == 0) {
          "You have pledged all your points."
        } else {
          sprintf("Last round you pledged: %g. Cap this round: %g.",
                  last_pledge_prev_round(), cap)
        }
      )
    )
  })

  observeEvent(input$submit_pledge, {
    req(isTRUE(authed()))
    if (!G$round_open) {
      showNotification("Pledging is currently closed.", type = "warning")
      return(NULL)
    }

    uid  <- user_id()
    disp <- dispname()

    req(input$pledge)
    pledge <- as.numeric(input$pledge)

    if (is_admin()) {
      pledge <- max(0, min(G$MAX_FOR_ADMIN, pledge))
    } else {
      pledge <- max(0, min(G$MAX_PER_STUDENT, pledge))
    }

    this_round <- G$round
    this_idx <- which(G$pledges$round == this_round & G$pledges$user_id == uid)

    timestamp_now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

    if (length(this_idx)) {
      G$pledges$pledge[this_idx] <- pledge
      G$pledges$name[this_idx]   <- disp
      G$pledges$when[this_idx]   <- timestamp_now
    } else {
      G$pledges <- bind_rows(
        G$pledges,
        tibble(
          round = this_round,
          user_id = uid,
          name = disp,
          pledge = pledge,
          charged = 0,
          when = timestamp_now
        )
      )
    }

    idxr <- which(G$pledges$round == this_round)
    sum_now <- sum(G$pledges$pledge[idxr])
    G$sum_pledge <- sum_now

    # auto timeout
    if (!is.na(G$ROUND_TIMEOUT_SEC) && !is.na(G$started_at)) {
      if ((as.numeric(Sys.time()) - G$started_at) >= G$ROUND_TIMEOUT_SEC) {
        G$round_open <- FALSE
      }
    }
  })

  output$my_submissions <- renderDT({
    req(isTRUE(authed()))
    uid <- user_id()
    me <- dplyr::filter(G$pledges, .data$user_id == uid) |>
      dplyr::select(round, name, pledge, charged, when) |>
      dplyr::arrange(dplyr::desc(round))
    if (nrow(me) == 0L) {
      me <- tibble::tibble(
        round = integer(), name = character(),
        pledge = numeric(), charged = double(), when = character()
      )
    }
    DT::datatable(me, options = list(pageLength = 5), rownames = FALSE)
  })

  # ===== ADMIN STATUS TEXT =====
  output$admin_round_title  <- renderText({
    sprintf("Round %g — Cost: %g points", G$round, G$COST)
  })

  output$admin_round_status <- renderText({
    paste(
      sprintf("Round open: %s | Questions unlocked (total): %s",
              G$round_open, G$unlocked_units),
      sprintf("Sum pledged this round: %.2f / %g | Sum charged this round: %.2f",
              G$sum_pledge, G$COST, G$sum_charged),
      if (!is.na(G$scale_factor)) {
        sprintf("Scale factor this round: %.3f", G$scale_factor)
      } else {
        "Scale factor: (n/a)"
      },
      sep = "\n"
    )
  })

  # ===== PROJECTOR UI =====
  output$projector_ui <- renderUI({
    req(isTRUE(authed()))

    # If the round is OPEN and the user is NOT an admin, hide projector contents
    if (isTRUE(G$round_open) && !isTRUE(is_admin())) {
      return(
        wellPanel(
          h4("Projector hidden until the round closes."),
          p(sprintf("Round %g is currently open. You’ll see results after it closes.", G$round))
        )
      )
    }

    # Otherwise (admin OR round closed), show projector
    fluidPage(
      h4("WTP (Current Round)"),
      fluidRow(
        column(8, plotOutput("wtp_hist", height = 320)),
        column(
          4,
          tags$div(style = "font-size: 1.1em; margin-top: 1em;", DTOutput("wtp_total")),
          tags$div(style = "margin-top: 0.5em;", textOutput("wtp_stats")),
          tags$hr()
        )
      ),
      tags$hr(),
      h5("Unlocked Questions"),
      uiOutput("projector_question")
    )
  })


  # Big reveal panel
  output$projector_question <- renderUI({
    if (G$unlocked_units > 0 && length(G$revealed_html) > 0) {
      tagList(
        h3(sprintf("Unlocked Questions (%g)", G$unlocked_units)),
        lapply(seq_along(G$revealed_html), function(i) {
          wellPanel(
            div(
              style="font-size:1.15em; line-height:1.5;",
              HTML(G$revealed_html[[i]])
            )
          )
        })
      )
    } else {
      wellPanel(
        h4("Question(s) locked"),
        p(sprintf(
          "Carryover from last round: %g; COST per question: %g.",
          G$carryover,
          G$COST
        )),
        p("Close the round to finalize purchases.")
      )
    }
  })

  output$wtp_total <- DT::renderDT({
    idxr <- which(G$pledges$round == G$round)
    pledged <- if (length(idxr)) sum(G$pledges$pledge[idxr]) else 0
    carryover <- if (G$round == 1) 0 else G$carryover
    effective_total <- pledged + carryover
    carryforward <- max(0, effective_total - G$COST)

    data <- data.frame(
      Metric = c(
        "This round pledged",
        "Carryover",
        "Effective total",
        sprintf("Carryforward (if over COST = %.2f)", G$COST)
      ),
      Value = c(
        sprintf("%.2f", pledged),
        sprintf("%.2f", carryover),
        sprintf("%.2f", effective_total),
        sprintf("%.2f", carryforward)
      )
    )

    datatable(
      data,
      rownames = FALSE,
      options = list(dom = 't', paging = FALSE, ordering = FALSE),
      colnames = c("", "")
    )
  })

  output$wtp_stats <- renderText({
    idxr <- which(G$pledges$round == G$round)
    n <- length(idxr)
    avg <- if (n) mean(G$pledges$pledge[idxr]) else 0
    sprintf("Participants: %g | Mean pledge: %.2f", n, avg)
  })

  output$wtp_hist <- renderPlot({
    idxr <- which(G$pledges$round == G$round)
    df <- if (length(idxr)) G$pledges[idxr, , drop = FALSE]
          else tibble::tibble(pledge = numeric())
    ggplot2::ggplot(df, ggplot2::aes(x = pledge)) +
      ggplot2::geom_histogram(
        binwidth = G$SLIDER_STEP,
        boundary = 0,
        closed = "left"
      ) +
      ggplot2::scale_x_continuous(
        limits = c(0, G$MAX_PER_STUDENT),
        breaks = seq(0, G$MAX_PER_STUDENT, by = G$SLIDER_STEP)
      ) +
      ggplot2::labs(x = "Pledge (WTP)", y = "Count") +
      ggplot2::theme_bw()
  })

  # ====== ADMIN ACTIONS: load / save / open / close / next / reset ======

  observeEvent(input$round_selector, ignoreInit = TRUE, {
    req(isTRUE(is_admin()))
    i <- as.integer(input$round_selector)
    updateTextAreaInput(
      session,
      "admin_question",
      value = title_from_html(QUESTIONS[[i]])
    )
  })

  observeEvent(input$save_texts, {
    req(isTRUE(is_admin()))
    G$question_text <- title_from_html(input$admin_question)
    showNotification("Question saved for this round.", type = "message")
  })

  observeEvent(input$open_round, ignoreInit = TRUE, {
    req(isTRUE(is_admin()))
    G$round_open <- TRUE
    G$question_unlocked <- FALSE
    # G$revealed_html <- character()
    G$sum_pledge <- 0
    G$sum_charged <- 0
    G$scale_factor <- NA_real_
    G$started_at <- as.numeric(Sys.time())

    # drop current-round rows; keep previous logs and carryover
    G$pledges <- dplyr::filter(G$pledges, round != G$round)

    showNotification(
      sprintf(
        "Pledging is OPEN for round %g. Carryover available: %g.",
        G$round, G$carryover
      ),
      type = "message"
    )
  })

  observeEvent(input$close_round, ignoreInit = TRUE, {
    req(isTRUE(is_admin()))
    if (!G$round_open) return(NULL)

    G$round_open <- FALSE

    # Current pledges this round
    idxr <- which(G$pledges$round == G$round)
    sum_now <- if (length(idxr)) sum(G$pledges$pledge[idxr]) else 0

    # Total available (carry + pledges)
    total_available <- G$carryover + sum_now

    # How many NEW questions can we buy?
    units_now <- as.integer(floor(total_available / G$COST))
    spend_now <- units_now * G$COST

    # Update cumulative unlocked
    G$unlocked_units <- G$unlocked_units + units_now

    # ====== CHARGING LOGIC ======
    if (length(idxr)) {
      if (identical(G$SHORTFALL_POLICY, "bank_all")) {
        # CHARGE EVERYONE FULLY THIS ROUND
        G$pledges$charged[idxr] <- round(G$pledges$pledge[idxr], 3)
        G$sum_charged <- sum(G$pledges$charged[idxr]) # this round (students)
        G$scale_factor <- 1
      } else if (identical(G$SHORTFALL_POLICY, "nocharge")) {
        if (units_now > 0) {
          # If at least one question was bought, you could choose proportional charge.
          # But the "bank_all" policy is the one you want. Keep this simple:
          G$pledges$charged[idxr] <- round(G$pledges$pledge[idxr], 3)
          G$sum_charged <- sum(G$pledges$charged[idxr])
          G$scale_factor <- 1
        } else {
          # Not funded → refund (charge 0)
          G$pledges$charged[idxr] <- 0
          G$sum_charged <- 0
          G$scale_factor <- NA_real_
        }
      }
    } else {
      G$sum_charged <- 0
      G$scale_factor <- NA_real_
    }

    # ====== CARRYOVER UPDATE ======
    # Whatever wasn't spent this becomes next round's carryover.
    # Under bank_all, sum_now has been charged and added to the class pot.
    # Pot size after charging = previous carryover + sum_now.
    # We spend 'spend_now' on questions. Remainder carries forward.
    G$carryover <- total_available - spend_now

    # ====== REVEAL QUESTIONS (if any purchased) ======
    if (units_now > 0) {
      start_idx <- as.integer(input$round_selector %||% G$round %||% 1L)
      k <- length(QUESTIONS)

      # show the cumulative pool on projector (most recent first)
      reveal_idx <- ((start_idx - 1L + seq_len(G$unlocked_units) - 1L) %% k) + 1L
      G$revealed_html <- rev(vapply(
        reveal_idx,
        function(i) as.character(QUESTIONS[[i]]),
        "",
        USE.NAMES = FALSE
      ))

      # For the student panel, show the first newly purchased one this round
      new_idxs <- ((start_idx - 1L + seq_len(units_now) - 1L) %% k) + 1L
      G$question_text <- as.character(QUESTIONS[[new_idxs[1]]])

      showNotification(
        sprintf("Purchased %g question(s). New carryover: %.2f.", units_now, G$carryover),
        type = "message"
      )
    } else {
      # No purchases this close
      if (identical(G$SHORTFALL_POLICY, "bank_all")) {
        # Bank pledges (already charged above) into carryover
        showNotification(
          sprintf("Not funded — pledges banked. New carryover: %.2f. At game over, carryover will be refunded proportionally.", G$carryover),
          type = "warning"
        )
      } else {
        # nocharge branch (unfunded): everyone charged 0 and nothing added to carryover
        showNotification("Not funded — no one charged; no carryover added.", type = "warning")
      }
    }
  })
  
  observeEvent(input$show_questions, ignoreInit = TRUE, {
    req(isTRUE(is_admin()))
    G$question_unlocked <- TRUE
    showNotification("Questions revealed.", type = "message")
  })


  observeEvent(input$next_round, ignoreInit = TRUE, {
    req(isTRUE(is_admin()))
    G$round <- if (G$round < length(QUESTIONS)) G$round + 1 else 1
    G$round_open <- FALSE
    G$question_unlocked <- FALSE
    G$revealed_html <- character()
    G$sum_pledge <- 0
    G$sum_charged <- 0
    G$scale_factor <- NA_real_
    G$started_at <- NA_real_
    G$question_text <- as.character(QUESTIONS[[G$round]])

    updateTextAreaInput(
      session,
      "admin_question",
      value = title_from_html(G$question_text)
    )

    showNotification(
      sprintf(
        "Moved to round %g. Carryover available: %g.",
        G$round, G$carryover
      ),
      type = "message"
    )
  })

  observeEvent(input$reset_all, ignoreInit = TRUE, {
    req(isTRUE(is_admin()))

    G$unlocked_units <- 0L
    G$carryover <- 0
    G$revealed_html <- character()

    G$round <- 1
    G$round_open <- FALSE
    G$question_unlocked <- FALSE
    G$sum_pledge <- 0
    G$sum_charged <- 0
    G$scale_factor <- NA_real_
    G$started_at <- NA_real_
    G$pledges <- G$pledges[0, ]
    G$question_text <- as.character(QUESTIONS[[1]])

    updateTextAreaInput(
      session,
      "admin_question",
      value = title_from_html(G$question_text)
    )

    showNotification(
      "All rounds and pledges reset (roster kept).",
      type = "error"
    )
  })

  observeEvent(input$end_game, ignoreInit = TRUE, {
    req(isTRUE(is_admin()))
    G$round_open <- FALSE

    # Compute proportional refunds of remaining carryover
    carry <- G$carryover
    if (carry > 0) {
      tc <- total_charged_by_user()
      tot <- sum(tc$charged, na.rm=TRUE)
      if (tot > 0) {
        tc$refund <- carry * (tc$charged / tot)
        # write refunds into a final “round 9999” ledger row (or a new table)
        timestamp_now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
        G$pledges <- dplyr::bind_rows(
          G$pledges,
          tc |>
            dplyr::transmute(
              round = 9999L,
              user_id, name,
              pledge = 0,
              charged = -refund,  # negative charge = refund
              when = timestamp_now
            ) 
        )|>
        group_by(user_id,name) |>
        mutate(total_charged = sum(charged,na.rm=TRUE))
        G$carryover <- 0
        showNotification(sprintf("Refunded %.2f proportionally.", sum(tc$refund)), type="message")
      } else {
        showNotification("No charges recorded; nothing to refund.", type="warning")
      }
    } else {
      showNotification("No carryover to redistribute.", type="message")
    }
  })
}

shinyApp(ui, server)
