# 🔍 JBoss Monitor Tool

A web dashboard to monitor and troubleshoot **JBoss EAP 8.1**. 🚀

It connects to a running EAP server through the management CLI (`jboss-cli.sh`) and shows
live data: threads, memory and GC, datasources, transactions, deployments, web sessions,
and more. It also has an offline **Root Cause Analysis** page where you upload log files and
it correlates them for you.

Works in both **standalone** and **domain** mode. ✅


## 📖 Contents

1. 🏁 How to use
2. ⚙️ What to enable before you use the tool (statistics & logs)
3. 📄 What each page shows
4. 🔬 Root Cause Analysis
5. 🛠️ Troubleshooting


---


# 🏁 1. How to use

## 📦 Step 1 — What you need

- Java 21
- Maven 3.9+
- A running JBoss EAP 8.1 server (standalone or domain)
- A JBoss management user (created with `add-user.sh`)
- `jboss-cli.sh` (comes with EAP, under `$JBOSS_HOME/bin`)
- (Optional) An Anthropic API key — only needed for the AI Analyst page


## 🔧 Step 2 — Prepare JBoss

Create a management user:

    $JBOSS_HOME/bin/add-user.sh
    # choose "Management User", set a username and password

Start EAP:

    # Standalone
    $JBOSS_HOME/bin/standalone.sh

    # Domain (run on the controller; start any slave hosts too)
    $JBOSS_HOME/bin/domain.sh


## ✏️ Step 3 — Configure

Open `set-env.txt` in the project root and set your values:

    export JBOSS_HOST=localhost
    export JBOSS_MGMT_PORT=9990
    export JBOSS_PROTOCOL=http
    export JBOSS_USER=<your-mgmt-user>
    export JBOSS_PASSWORD=<your-mgmt-password>
    export JBOSS_HOME=/path/to/jboss-eap-8.1

    # Domain mode only: map each host name to its install path
    export JBOSS_DOMAIN_HOMES="primary=/path/to/eap,slave=/path/to/eap-slave"

    # Optional
    export LOG_LEVEL=INFO
    export ANTHROPIC_MODEL=claude-sonnet-4-6
    export ANTHROPIC_BASE_URL=<your-anthropic-endpoint>
    export ANTHROPIC_API_KEY=<your-key>

Note: `set-env.txt` contains your password and API key. Do not share it or commit it.

All settings:

| Setting | Default | What it does |
|---------|---------|--------------|
| JBOSS_HOST | localhost | Controller host |
| JBOSS_MGMT_PORT | 9990 | Management port |
| JBOSS_PROTOCOL | http | http or https |
| JBOSS_USER / JBOSS_PASSWORD | admin / admin | Management login |
| JBOSS_HOME | — | EAP install folder |
| JBOSS_DOMAIN_HOMES | — | host=path pairs, for domain logs |
| LOG_LEVEL | INFO | Log detail |
| ANTHROPIC_* | — | AI Analyst only |
| JBOSS_CLI_POOL_SIZE | 4 | How many CLI connections to keep open |
| JBOSS_CLI_PROBE_TIMEOUT_MS | 800 | How long to wait when checking JBoss is up |


## ▶️ Step 4 — Run

    source set-env.txt
    mvn quarkus:dev

Then open **http://localhost:8000** in your browser.

- The port is always 8000.
- Changes to code or pages reload automatically.
- Logs are saved to `logs/jboss-monitor.log`.

To build and run a JAR instead:

    mvn clean package
    java -jar target/quarkus-app/quarkus-run.jar     # after: source set-env.txt


## 🖧 Step 5 — Choosing a server (domain mode)

By default, pages show data from all running servers combined.

To look at just one server, add this to the page URL:

    ?host=<host>&server=<server>

Example:

    http://localhost:8000/threads.html?host=primary&server=server-one


---


# ⚙️ 2. What to enable before you use the tool (statistics & logs)

A feature needs one of two things from JBoss:

- 📊 **Statistics** — a `statistics-enabled` flag on a subsystem. Without it the page loads but
  shows zeros. ✨ **The tool turns these on for you** (see 2.1), so usually there's nothing to do.
- 📁 **Logs** — a log file on disk that the tool reads. Some are on by default; some you turn on
  yourself (see 2.3).

Everything else reads the JVM MBeans and management model, which are always there — so those
pages just work once JBoss is up. ✅

## 🗺️ 2.0 Capability → what it needs

One row per page/section. "Automatic" = the tool enables it; "Default" = EAP already has it on;
"You enable it" = run the commands in the linked subsection.

| Page / section | Needs | Category | Who enables it |
|----------------|-------|----------|----------------|
| Dashboard, Server Status, Domain Topology | root + platform MBeans | Always on | — |
| Memory & GC (page) | platform MBeans | Always on | — |
| Threads → State, Details, Dump, Stack | platform MBeans (threading) | Always on | — |
| Threads → Thread Pools (Undertow/IO/EJB/JCA) | subsystem runtime | Always on | — |
| Deployments | management model | Always on | — |
| **Datasources → pool statistics** | datasources `statistics-enabled` | **Statistics** | Automatic (2.1) |
| **Transactions → stats & distribution** | transactions `statistics-enabled` | **Statistics** | Automatic (2.1) |
| **Web & Sessions → listeners, servlets, sessions** | undertow `statistics-enabled` | **Statistics** | Automatic (2.1) |
| **Threads → Undertow Listener Stats** | undertow `statistics-enabled` | **Statistics** | Automatic (2.1) |
| **Deployments → per-WAR sessions/servlets; Memory & GC → Deployment Memory Usage** | undertow `statistics-enabled` | **Statistics** | Automatic (2.1) |
| Server Logs page, RCA Error Correlation | `server.log` | Log (default) | Default (2.3.1) |
| Memory & GC → RCA GC Analysis | `gc.log` | Log (default) | Default standalone (2.3.2) |
| **API Data timing, RCA Slow APIs** | Undertow **access log** | **Log** | You enable it (2.3.3) |
| **JDBC Spy page, RCA JDBC Analysis** | **JDBC Spy** log | **Log** | You enable it (2.3.4) |
| **Transactions → History, RCA Transaction Analysis** | **`com.arjuna` TRACE** log | **Log** | You enable it (2.3.5) |
| Threads → Dump, RCA Thread Analysis | thread dump | On demand | Captured/uploaded (2.3.6) |

In **domain mode**, statistics are enabled per profile automatically. If you enable a log
manually, add `/profile=<your-profile>` in front of each `/subsystem=...` command and reload the
affected servers instead of the standalone `reload`.


## 📊 2.1 Statistics — enabled automatically

When the tool starts it turns on the statistics the pages need — normally **nothing for you to
do**. On startup it sets `statistics-enabled=true` on:

- the **undertow** subsystem — powers Web & Sessions (HTTP listeners, servlet performance,
  active sessions) and the Undertow Listener Stats on the Threads page;
- the **transactions** subsystem — powers the Transactions page (committed / rolled back /
  in-flight counts and the distribution charts);
- every **data-source** and **xa-data-source** — powers Datasource pool statistics (active,
  idle, max connections and pool details).

In **standalone** mode it applies these to the running server; in **domain** mode it applies
them to every configured profile.

Two things to know:

- **The management user must be allowed to write.** The tool uses `write-attribute`, so a
  read-only role (e.g. `Monitor`) will leave statistics off. Use a management user with
  `Administrator`/`SuperUser` (or at least `Maintainer`) rights, or enable the flags yourself
  once (below).
- **If a page shows all zeros**, the flag is probably off (write failed, or was reset by a
  config restore). Confirm or set it manually — these take effect immediately, no reload needed:

      # Standalone
      /subsystem=undertow:write-attribute(name=statistics-enabled,value=true)
      /subsystem=transactions:write-attribute(name=statistics-enabled,value=true)
      /subsystem=datasources/data-source=<YourDataSource>:write-attribute(name=statistics-enabled,value=true)

      # Domain (per profile)
      /profile=<your-profile>/subsystem=undertow:write-attribute(name=statistics-enabled,value=true)
      /profile=<your-profile>/subsystem=transactions:write-attribute(name=statistics-enabled,value=true)
      /profile=<your-profile>/subsystem=datasources/data-source=<YourDataSource>:write-attribute(name=statistics-enabled,value=true)

KCS: How to enable statistics for the undertow subsystem — https://access.redhat.com/solutions/2757581
KCS: Enable datasource pool statistics in JBoss EAP — https://access.redhat.com/solutions/29282


## 📁 2.2 About the log capabilities

The file-based features — JDBC Spy, Transaction History, access-log API timing, and the whole
Root Cause Analysis page — only show data if the matching EAP log is on first.

Turn on the ones you need with the commands in 2.3. Each links a Red Hat Knowledgebase (KCS)
article (a Red Hat login is needed to read them in full).

| Log | Needed for | Enabled by default? |
|-----|-----------|---------------------|
| server.log | Server Logs page, RCA error correlation | Yes |
| Garbage collection (gc.log) | Memory & GC page, RCA GC Analysis | Yes (standalone) |
| Undertow access log (access.log) | RCA Slow APIs, API Data | No |
| JDBC Spy (jdbc-spy.log) | JDBC Spy page, RCA JDBC Analysis | No |
| Transaction log (com.arjuna) | Transactions page, RCA Transaction Analysis | No (WARN only) |
| Thread dump | Threads page, RCA Thread Analysis | Captured on demand |


## 🔌 2.3 Enabling each log

### 📝 2.3.1 server.log — already on
The main server log is enabled by default. No action needed.

### 🗑️ 2.3.2 Garbage collection log — usually already on
For a standalone server, GC logging is on by default and written to
`EAP_HOME/standalone/log/gc.log`. Red Hat recommends leaving it on.

For Java 21 the log flags use the unified format. To set them yourself, edit
`bin/standalone.conf` (or `bin/domain.conf`) and add:

    -Xlog:gc*:file=/path/to/gc.log:time,uptime:filecount=5,filesize=3M

KCS: How do I enable Java garbage collection logging? — https://access.redhat.com/solutions/18656
KCS: How to enable GC logging in JBoss EAP domain mode — https://access.redhat.com/solutions/269023

### 🌐 2.3.3 Undertow access log — off by default
Turn it on so request URLs and **response times** are recorded.

RCA reads access logs in a format-agnostic way, so you do not have to use one exact pattern.
It auto-detects the common layouts:

- The default `common` and `combined` patterns.
- Custom patterns in almost any field order.
- Labeled `Key: value` fields, quoted or not — e.g. `Thread: "default task-1"`,
  `TimeTaken: 0.023`, `SessionID: -`. The worker thread is read from a `Thread:` label even
  when its value is quoted, so it still links a slow request to its thread stack.
- Response time as milliseconds (`%D`), seconds (`%T`, e.g. `0.007`), or unit-suffixed
  (`4200ms`, `0.5s`).
- Common (`[17/Aug/2026:10:00:00 +0000]`) and ISO-8601 (`2026-08-17T10:00:00+00:00`) timestamps.
- JSON access logs (fields like `requestPath`, `responseCode`, `responseTime`, `bytesSent`).

The one thing RCA cannot invent is a response time that the log never recorded. The Slow APIs
tab needs a per-request time, so the pattern must include one (`%D`, `%T`, or a unit-suffixed
time). The default `common`/`combined` patterns do NOT include a response time — with them RCA
still lists endpoints, status codes and errors, but cannot rank slow APIs.

Recommended pattern (response time in ms, plus the worker thread for stack correlation):

    /subsystem=undertow/server=default-server/http-listener=default:write-attribute(name=record-request-start-time,value=true)
    /subsystem=undertow/server=default-server/host=default-host/setting=access-log:add
    (pattern="%h %l %u %t \"%r\" %s %b \"%{i,Referer}\" \"%{i,User-Agent}\" Cookie: \"%{i,COOKIE}\" Set-Cookie: \"%{o,SET-COOKIE}\" SessionID: %S Thread: \"%I\" TimeTaken: %T")

   KCS: Enabling Access log in JBoss EAP :  https://access.redhat.com/solutions/2423311


(If access logging is already enabled, skip the `:add` line and just run the
`write-attribute` + `reload`.)

The pattern fields, in order, are:

| Token | Meaning |
|-------|---------|
| `%h` | Remote host |
| `%t` | Timestamp |
| `%r` | Request line (method, URI, protocol) |
| `%s` | HTTP status |
| `%D` | Response time in milliseconds — required for Slow APIs |
| `%b` | Bytes sent |
| `%I` | Worker thread name — lets RCA link a slow request to its thread stack |

The log is written to `standalone/log/access_log.log`. In domain mode, prefix the commands
with `/profile=<your-profile>`.

Tip on field order: when a pattern has both a bare-integer response time and bytes (e.g.
`%D %b` vs `%b %D`), RCA works out which column is which by noticing that the bytes column
sometimes shows `-` for zero-byte responses. If your log never has a zero-byte response, RCA
assumes the response time comes first (`%D %b`, as in the recommended pattern above). Using
seconds (`%T`) or a unit suffix (`%D` written as `4200ms`) removes any ambiguity entirely.

KCS: How to enable access logging for JBoss EAP 7/8 — https://access.redhat.com/solutions/2423311

### 🗄️ 2.3.4 JDBC Spy (SQL logging) — off by default
This tool reads a **separate** `jdbc-spy.log` file (standalone: `standalone/log/jdbc-spy.log`;
domain: `<host>/domain/servers/<server>/log/jdbc-spy.log`). If you only add the
`jboss.jdbc.spy` logger, its output inherits the root handlers and gets **mixed into
`server.log`** — the tool then cannot find a dedicated spy log and the JDBC Spy page reports
"not configured".

So three things are required: turn on `spy` for the datasource, give the spy logger its **own
periodic-rotating-file-handler** pointed at `jdbc-spy.log`, and route the logger to that handler
only (`use-parent-handlers=false`) so nothing goes to `server.log`:

    # 1. Enable spy on each datasource you want to trace
    /subsystem=datasources/data-source=<YourDataSource>:write-attribute(name=spy,value=true)

    # 2. Dedicated rotating file handler -> jdbc-spy.log (relative-to resolves per server)
    /subsystem=logging/periodic-rotating-file-handler=jdbc-spy:add(level=TRACE, append=true, autoflush=true, suffix=".yyyy-MM-dd", file={relative-to=jboss.server.log.dir, path=jdbc-spy.log}, formatter="%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c] (%t) %s%e%n")

    # 3. Send the spy logger to that handler ONLY (keeps it out of server.log)
    /subsystem=logging/logger=jboss.jdbc.spy:add(level=TRACE, use-parent-handlers=false, handlers=[jdbc-spy])

    reload

The filename must be exactly `jdbc-spy.log` — that is what the tool looks for.

If you already added the `jboss.jdbc.spy` logger before (the old single-line way), don't run its
`:add` again — modify it instead so it stops writing to `server.log`:

    /subsystem=logging/periodic-rotating-file-handler=jdbc-spy:add(level=TRACE, append=true, autoflush=true, suffix=".yyyy-MM-dd", file={relative-to=jboss.server.log.dir, path=jdbc-spy.log}, formatter="%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c] (%t) %s%e%n")
    /subsystem=logging/logger=jboss.jdbc.spy:write-attribute(name=handlers,value=[jdbc-spy])
    /subsystem=logging/logger=jboss.jdbc.spy:write-attribute(name=use-parent-handlers,value=false)
    /subsystem=logging/logger=jboss.jdbc.spy:write-attribute(name=level,value=TRACE)
    reload

In **domain mode**, prefix each `/subsystem=logging/...` command with `/profile=<your-profile>`
and reload the affected servers. `relative-to=jboss.server.log.dir` automatically resolves to
each server's own `log/` directory, so every server writes its own `jdbc-spy.log`.

Note: this is verbose. Turn it on only while diagnosing, then turn it off.

KCS: Enabling DataSource Spy Logging in JBoss EAP 6/7 — https://access.redhat.com/solutions/89393
KCS: How to create a separate log file for a specific logger in JBoss EAP — https://access.redhat.com/solutions/209403

### 🔁 2.3.5 Transaction logging — set to TRACE (default is WARN)
The `com.arjuna` logger controls transaction detail. Raise it to TRACE (or DEBUG for less noise):

    /subsystem=logging/logger=com.arjuna:write-attribute(name=level,value=TRACE)

For full JCA/transaction troubleshooting, Red Hat suggests also enabling `org.jboss.jca` and
`org.jboss.as.connector` at TRACE.

KCS: How to add JCA/Transaction TRACE level logging in JBoss EAP — https://access.redhat.com/solutions/287523

### 🧵 2.3.6 Thread dump — no setup needed
The Threads page can take a dump on demand, and RCA accepts a thread-dump file you upload
(including `jstack` loop files with many snapshots). Nothing to enable.


---


# 📄 3. What each page shows

Open pages from the left menu. Most need a live JBoss connection; the **Root Cause Analysis**
page does not.

| Page | Address | In short |
|------|---------|----------|
| Dashboard | /index.html | Overall health and quick links |
| Server Status | /server-status.html | Server identity, JVM and OS info |
| Domain Topology | /domain.html | Hosts, servers and their state |
| Threads | /threads.html | Thread health, pools and dumps |
| Memory & GC | /memory.html | Heap, memory pools and GC |
| Datasources | /datasources.html | Connection pool health |
| JDBC Spy | /jboss-datasource.html | Captured SQL queries |
| Transactions | /transactions.html | Transaction stats and history |
| Web & Sessions | /web-stats.html | HTTP listeners and sessions |
| Deployments | /deployments.html | Deployed apps and status |
| Server Logs | /server-logs.html | EAP log files with error hints |
| API Data | /api-data.html | Request timing and recent calls |
| Root Cause Analysis | /rca.html | Offline log analysis |
| AI Analyst | /ai-analyst.html | AI narrative of collected data |


## 🏠 Dashboard  — /index.html
The home page and a quick health check.
- Health Overview: is the server up, and top-level warnings.
- Quick Actions: shortcuts to common pages.
- System Information: server and JVM summary.

## ⚙️ Server Status  — /server-status.html
Basic facts about the server.
- Server Information: mode, product version, running state, uptime.
- JVM Runtime: Java version, vendor, start time.
- System Information: OS, CPUs, system load.
- JVM Arguments: the startup flags.
- Ask about server: an AI explanation of the current state.

## 🌐 Domain Topology  — /domain.html  (domain mode)
The layout of your domain.
- Server Groups: each host, its servers, their group, and whether each is running or stopped.

## 🧵 Threads  — /threads.html
Thread health and thread dumps. In domain mode this combines all servers.
- Thread State Distribution: how many threads are RUNNABLE, WAITING, BLOCKED, etc.
- Thread Details: each thread's category, task, top stack frame, and lock info.
- Thread Pools: Undertow, IO, EJB and JCA pools with their sizes and usage.
- Undertow Listener Stats: requests, timing and bytes per listener.
- Thread Stack Trace: click any thread to see its full stack.
- Thread Dump: take a live snapshot of all threads.
- Capture Thread Dumps to Disk: saves dumps to the server (local host only).
- Thread Event Log: past blocked and deadlock events.

## 💾 Memory & GC  — /memory.html
JVM memory and garbage collection.
- Heap Memory and Non-Heap Memory: used, committed and max, with bars.
- Memory Pools: Eden, Survivor, Old Gen, Metaspace, and so on.
- Garbage Collectors: how often GC runs and total GC time.
- Deployment Memory Usage: a per-WAR card for each application showing its **estimated** share of
  memory (a relative estimate — see the Deployments page notes) together with that WAR's session,
  servlet, request and JPA stats. Handy for spotting which app is the heaviest.
- Generate Heap Dump: creates a heap dump on the local JBoss process (open the `.hprof` in a heap
  analyzer for exact per-object memory).

## 🛢️ Datasources  — /datasources.html
Database connection pool health.
*Requires datasource statistics — enabled automatically (see 2.1).*
- Datasource Overview: your data sources.
- Connection Pool Utilization: active, idle and max connections.
- Pool Details: full pool statistics.
- Pool Actions: pool operations such as flush.
- Query Monitoring and Thread Details: which threads and queries use the pool.

## 🗄️ JDBC Spy  — /jboss-datasource.html
*Requires the JDBC Spy log — you enable it (see 2.3.4).*
- JDBC Spy Query Log: the SQL statements captured, with timing, to help find slow queries.

## 🔁 Transactions  — /transactions.html
Transaction activity.
*Stats & distribution require transaction statistics — enabled automatically (see 2.1).
Transaction History additionally needs the `com.arjuna` TRACE log — you enable it (see 2.3.5).*
- Transaction Distribution: committed vs rolled back vs in progress.
- Rollback Breakdown: why transactions rolled back.
- Transaction Details: live numbers.
- Transaction History: activity over time.
- Datasource Pool Sessions: database sessions tied to transactions.
- Thread Details: threads running transactions.

## 🕸️ Web & Sessions  — /web-stats.html
Web layer and HTTP sessions.
*Requires undertow statistics — enabled automatically (see 2.1).*
- HTTP Listeners: stats per listener.
- Request Throughput by Listener: request rate.
- HTTP Sessions: active sessions per app.
- Servlet Performance: per-servlet counts and times.
- Session History: sessions over time.
- Individual Session Details: look at one session.
- Thread Stack Trace: the thread handling a request.

## 📦 Deployments  — /deployments.html
- Deployments: your applications (WARs/EARs) and whether they are enabled and running.
- Deployment Details: sub-deployments, context roots, and per-server status.
- Per-WAR servlet & session stats: expand a deployment to see its servlets (request counts and
  times) and its HTTP **session** figures — Active Sessions, Created Sessions, Expired Sessions.
  *Requires undertow statistics — enabled automatically (see 2.1) — and the app must actually
  create HTTP sessions.*

### 🔎 Checking a WAR's heap usage and sessions

**👥 Session info per WAR** — three places show it, pick whichever you're already on:
- **Deployments** page — expand the deployment card (Active / Created / Expired sessions +
  servlet stats). Best when you want it alongside the deployment's status.
- **Memory & GC** page → **Deployment Memory Usage** — each WAR card lists Active Sessions,
  Sessions Created, Total Requests, Request Time, Servlets, REST Endpoints, and JPA stats.
- **Web & Sessions** page → **HTTP Sessions** — the fullest view (active, expired, created, max
  active, average/max alive time) per app, plus per-session drill-down.

**💾 Heap / memory usage per WAR** — open **Memory & GC** → **Deployment Memory Usage**. Each WAR
gets a bar showing its share of the total. ⚠️ Note: JBoss does **not** report an exact per-WAR heap
figure (all apps share one JVM heap), so this is a **relative estimate** built from each WAR's
sessions, servlets, REST endpoints, EJBs and JPA entities / L2 cache. Use it to compare which apps
are heavier, not as an exact byte count. For real JVM heap and GC pressure use the Heap Memory,
Memory Pools and Garbage Collectors sections; for an exact per-object breakdown use **Generate Heap
Dump** and open the `.hprof` in a heap analyzer (e.g. Eclipse MAT).

## 📜 Server Logs  — /server-logs.html
Reads the EAP log files from disk.
- Error Codes & AI Solutions: pulls out WFLY error codes and suggests fixes.
- Server Log Entries: a searchable log viewer.

## ⚡ API Data  — /api-data.html
Request tracking collected by the tool.
*Endpoint response times need the Undertow access log with a response-time token — you enable it
(see 2.3.3). Active/recent request tracking works without it.*
- Deployment Summary: requests and errors per app.
- Endpoint Response Times: how slow each endpoint is.
- API Endpoints: the endpoints seen and their stats.
- Recent API Requests: the latest requests and the thread running each.
- Active API Requests: requests being processed right now, with a live **Processing Time**.
- Thread Details: threads currently serving requests.

The URL and handler shown for an active request come from the thread's **live stack**, not the
access log. The access log only records requests that have *finished*, so a worker thread that is
reused between requests would otherwise be labelled with its previous, unrelated call (e.g. an
in-flight upload showing an earlier download's URL).

For a request handled by a `@WebServlet` servlet, the exact URL is not in JBoss's management model
(annotation-mapped servlets report no mapping). The tool recovers the real path two ways and shows
the servlet class alongside it — e.g. `POST /my-app/api/files/upload (FileUploadServlet.doPost)`:

- **Correlation** — the background poller remembers each in-flight request (worker thread + start
  time + handler); when that request later completes and is written to the access log on the same
  thread at the same start instant, the tool learns "this servlet serves this URL" and uses it from
  then on. This is the same thread + time-window correlation the RCA page uses.
- **Name hint** — until a request of that servlet has completed at least once, the tool matches the
  servlet's class name against the URLs already seen for its deployment (e.g. `FileUploadServlet`
  → the `POST .../upload` path), used only when exactly one URL matches.

If neither resolves it, the deployment context root with a wildcard is shown (`/my-app/**`) rather
than a misleading stale URL.

The Active API Requests section updates on its own — you do not need to reload the page. The
Processing Time ticks live every 0.5 second in the browser (advanced from the wall clock, so it
counts smoothly without extra load on JBoss), the request list itself is fetched every 5 seconds,
and the rest of the page every 30 seconds.

The **Processing Time** for an active request counts from when the request actually started
being processed — not from when you opened the page. The tool samples in-flight requests in
the background (every 5s by default) so a request already running when the page loads still
shows its true elapsed time (accurate to within one sample). Tune the interval with
`monitor.active-requests.poll-interval` in `application.properties` (lower is more accurate but
dumps threads from JBoss more often).

## 🔬 Root Cause Analysis  — /rca.html   (offline)
Upload log files and get a combined diagnosis. See section 4 below.

## 🤖 AI Analyst  — /ai-analyst.html   (needs Anthropic key)
- AI-Powered Analysis: sends the collected data to Claude and shows a written diagnosis.
- Analysis History: your past analyses.


---


# 🔬 4. Root Cause Analysis

No live JBoss connection needed. You give it log files and it links them together by thread name
and time.

Steps:

1. Open http://localhost:8000/rca.html
2. Upload any mix of these files:
   - thread dump
   - server.log
   - access.log
   - GC log
   - jdbc-spy.log
   - transaction.log
3. Click **Analyze**.

You then get tabs for: Findings, Slow APIs, Thread Analysis, Error Correlation, GC Analysis,
JDBC Spy Analysis, Transaction Analysis, and an Event Timeline.

Tips:
- A thread dump file often holds many snapshots taken seconds apart. The tool splits them and
  shows the latest by default. Use the snapshot picker in Thread Analysis to switch.
- Click any thread name to see its full stack trace.
- Uploaded files stay cached in your browser after a refresh. If you see data from an old file,
  click Remove on that slot, upload the new file, then Analyze again.


---


# 🛠️ 5. Troubleshooting

**Pages are empty, or "Cannot read root resource" — often right after restarting JBoss.**
The tool keeps CLI connections open, and a restart can make them stale. It now reconnects on
its own, so just reload the page. If JBoss is actually down you will see a clear message
"controller not reachable — is JBoss running?". You can test the connection directly:

    $JBOSS_HOME/bin/jboss-cli.sh --connect --controller=$JBOSS_HOST:$JBOSS_MGMT_PORT \
      --user=$JBOSS_USER --password=$JBOSS_PASSWORD "/:read-resource"

If that prints `"outcome" : "success"`, the tool will work too. If not, the problem is with
JBoss or your credentials.

**"CLI did not produce prompt within 15s".**
`jboss-cli.sh` could not be found or could not connect. Check that JBOSS_HOME points to a real
EAP install and that the management port is reachable.

**RCA shows old data after uploading a new file.**
Click Remove on the file slot, upload again, then click Analyze. (The browser caches files.)

**AI Analyst gives an error.**
Set ANTHROPIC_BASE_URL, ANTHROPIC_MODEL and ANTHROPIC_API_KEY. This page is optional.

**Where are the logs?**
`logs/jboss-monitor.log`. Set LOG_LEVEL=DEBUG in set-env.txt for more detail.


---


# 📝 Notes

- Built with Quarkus 3.37.2, Java 21 and Maven.
- All JBoss data is read through `jboss-cli.sh` (not the HTTP management API). The only
  outbound HTTP is the optional AI Analyst calling Anthropic.
- The web pages are plain HTML/CSS/JS.
- The UI always runs on port 8000. RCA accepts uploads up to 350 MB.
