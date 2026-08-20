/* Astro console.
 *
 * Served by the Pi's own nginx, talking to the Pi's own mosquitto over
 * websockets. Nothing here is site-specific: the broker is derived from the
 * address you loaded the page from, so the identical file works at Dangar and
 * at the cabin without configuration. That is the point - two Node-RED flows
 * drift apart, one file in the repo cannot.
 *
 * Archive and render are not Pi operations (the archive lives on the LXD and
 * ffmpeg runs there), so those are published as MQTT commands for Node-RED,
 * which already subscribes to this broker.
 */
(function () {
  "use strict";

  var $ = function (id) { return document.getElementById(id); };
  var PREFIX = "astrophoto";
  var host = location.hostname;
  var WS = "ws://" + host + ":9001";

  $("site").textContent = host;

  /* ── state ─────────────────────────────────────────────────────────────── */
  var PARAMS = {};        // profile name -> {exposure, iso, frames, interval, duration}
  var BUILTIN = [];
  var CURRENT = { name: null, p: {} };   // what a capture will actually use
  var CAM = null;
  var CAPTURING = false;

  var FIELD_IDS = ["exp", "iso", "frm", "ivl", "dur"];
  /* form field -> the key the bridge uses */
  var KEY = { exp: "exposure", iso: "iso", frm: "frames", ivl: "interval", dur: "duration" };

  function el(id) { return $(id); }
  function fields() { return FIELD_IDS.map(function (f) { return [f, $(f)]; }); }

  function toast(m) {
    var t = $("toast");
    t.textContent = m; t.hidden = false;
    clearTimeout(toast._t);
    toast._t = setTimeout(function () { t.hidden = true; }, 2600);
  }

  /* ── mqtt ──────────────────────────────────────────────────────────────── */
  var client = mqtt.connect(WS, {
    reconnectPeriod: 3000,
    connectTimeout: 8000,
    clientId: "console_" + Math.random().toString(16).slice(2, 10)
  });

  function pub(topic, obj) {
    client.publish(PREFIX + "/" + topic, JSON.stringify(obj || {}));
  }

  client.on("connect", function () {
    $("conn").textContent = "connected";
    $("conn").classList.add("ok");
    client.subscribe(PREFIX + "/status");
    client.subscribe(PREFIX + "/event/#");
    pub("query/profiles");
    pub("query/status");
    pub("query/camera");
    pub("query/exposures");
    pub("query/isos");
    loadSessions();
  });

  client.on("reconnect", function () {
    $("conn").textContent = "reconnecting…";
    $("conn").classList.remove("ok");
  });
  client.on("close", function () {
    $("conn").textContent = "disconnected";
    $("conn").classList.remove("ok");
  });
  client.on("error", function (e) {
    $("conn").textContent = "error: " + (e && e.message ? e.message : e);
    $("conn").classList.remove("ok");
  });

  client.on("message", function (topic, buf) {
    var sub = topic.slice(PREFIX.length + 1), d;
    try { d = JSON.parse(buf.toString()); } catch (e) { return; }

    if (sub === "status") {
      CAPTURING = d.state === "capturing";
      setState(d);
    } else if (sub === "event/profiles") {
      PARAMS = d.params || {};
      BUILTIN = d.builtin || [];
      fillProfileList();
    } else if (sub === "event/camera") {
      CAM = d;
      showCamera(d);
      fillLadders();
    } else if (sub === "event/exposures") {
      fillSelect($("exp"), d.exposures || d.speeds || []);
    } else if (sub === "event/isos") {
      fillSelect($("iso"), d.isos || []);
    } else if (sub === "event/frame") {
      CAPTURING = true;
      $("state").textContent = "Capturing " + d.frame + " / " + d.total;
      $("state").classList.add("on");
      $("abort").disabled = false;
    } else if (sub === "event/complete") {
      CAPTURING = false;
      $("state").textContent = "Idle";
      $("state").classList.remove("on");
      $("abort").disabled = true;
      toast(d.frames + " frame" + (d.frames === 1 ? "" : "s") + " complete");
      loadSessions();
    } else if (sub === "event/aborted") {
      CAPTURING = false;
      $("state").textContent = "Idle";
      $("state").classList.remove("on");
      $("abort").disabled = true;
      toast("Aborted at frame " + d.frame);
      loadSessions();
    } else if (sub === "event/preview") {
      showShot(d.path);
    } else if (sub === "event/error") {
      var e = $("err");
      e.hidden = false; e.className = "msg bad";
      e.textContent = d.message;
      CAPTURING = false;
      $("abort").disabled = true;
      $("state").textContent = "Idle";
      $("state").classList.remove("on");
    } else if (sub === "event/info") {
      toast(d.message);
    }
  });

  function setState(d) {
    $("state").textContent = CAPTURING ? "Capturing" : "Idle";
    $("state").classList.toggle("on", CAPTURING);
    $("abort").disabled = !CAPTURING;
    $("fire").disabled = CAPTURING;
    if (d && d.camera) $("c-body").textContent = d.camera;
  }

  function showCamera(d) {
    $("c-body").textContent = d.camera || "—";
    if (d.min_exposure != null) {
      $("c-exp").textContent = fmtExp(d.min_exposure) + " – " + fmtExp(d.max_exposure);
    }
    if (d.min_iso != null) $("c-iso").textContent = d.min_iso + " – " + d.max_iso;
  }

  function fmtExp(v) {
    v = Number(v);
    if (v > 0 && v < 1) return "1/" + Math.round(1 / v);
    return String(v);
  }

  function showShot(path) {
    var file = String(path).split("/").pop();
    var wrap = $("shotwrap");
    wrap.innerHTML = "";
    var img = new Image();
    img.className = "shot";
    img.alt = file;
    img.src = "/" + file + "?t=" + Date.now();
    img.onerror = function () { wrap.textContent = "Preview not readable: " + file; };
    wrap.appendChild(img);
  }

  /* ── profiles ──────────────────────────────────────────────────────────── */
  function isSeq(p) {
    var iv = parseFloat(p.interval) || 0;
    return iv > 0 && ((parseFloat(p.duration) || 0) > 0 || (parseInt(p.frames, 10) || 0) > 0);
  }

  function fillProfileList() {
    var sel = $("profile"), keep = sel.value;
    sel.innerHTML = "";
    var groups = { "Single & burst": [], "Timelapse": [] };
    Object.keys(PARAMS).sort().forEach(function (n) {
      groups[isSeq(PARAMS[n]) ? "Timelapse" : "Single & burst"].push(n);
    });
    Object.keys(groups).forEach(function (g) {
      if (!groups[g].length) return;
      var og = document.createElement("optgroup");
      og.label = g;
      groups[g].forEach(function (n) {
        var o = document.createElement("option");
        o.value = n; o.textContent = n;
        og.appendChild(o);
      });
      sel.appendChild(og);
    });
    if (keep && PARAMS[keep]) sel.value = keep;
    if (!CURRENT.name) {
      CURRENT = { name: sel.value, p: copy(PARAMS[sel.value] || {}) };
    }
    fill();
    refreshForm();
    refreshCurrent();
  }

  function copy(o) { var n = {}; for (var k in o) n[k] = o[k]; return n; }

  function fillSelect(sel, values) {
    if (!values.length) return;
    var keep = sel.value;
    sel.innerHTML = "";
    values.forEach(function (v) {
      var o = document.createElement("option");
      o.value = String(v); o.textContent = String(v);
      sel.appendChild(o);
    });
    if (keep) sel.value = keep;
  }

  /* Fallback ladders if the bridge does not answer the queries. Kept coarse on
     purpose - the authoritative list comes from the camera. */
  function fillLadders() {
    if (!$("exp").options.length) {
      fillSelect($("exp"), ["1/4000","1/1000","1/500","1/250","1/125","1/60",
                            "0.01","0.5","1","2","4","8","15","20","30","60","120","300"]);
    }
    if (!$("iso").options.length) {
      fillSelect($("iso"), [100,200,400,800,1600,3200,6400,12800,32000]);
    }
  }

  function profileValue(p, f) {
    var v = p[KEY[f]];
    return v === undefined || v === null ? "" : String(v);
  }

  function fill() {
    var p = PARAMS[$("profile").value] || {};
    fields().forEach(function (pair) {
      var want = profileValue(p, pair[0]);
      /* an exposure like 20 must match an option; add it if the ladder lacks it */
      if (pair[1].tagName === "SELECT" && want &&
          !Array.prototype.some.call(pair[1].options, function (o) { return o.value === want; })) {
        var o = document.createElement("option");
        o.value = want; o.textContent = want;
        pair[1].insertBefore(o, pair[1].firstChild);
      }
      pair[1].value = want;
      $("l-" + pair[0]).classList.remove("edited");
    });
    $("savename").value = $("profile").value;
    $("savebar").hidden = true;
  }

  function formValues() {
    var o = {};
    fields().forEach(function (pair) {
      var v = pair[1].value.trim();
      if (v !== "") o[KEY[pair[0]]] = isNaN(v) ? v : Number(v);
    });
    return o;
  }

  function sameParams(a, b) {
    var keys = ["exposure", "iso", "frames", "interval", "duration"];
    return keys.every(function (k) {
      return String(a[k] === undefined ? "" : a[k]) === String(b[k] === undefined ? "" : b[k]);
    });
  }

  function edits() {
    var p = PARAMS[$("profile").value] || {}, any = false;
    fields().forEach(function (pair) {
      var diff = pair[1].value.trim() !== profileValue(p, pair[0]);
      $("l-" + pair[0]).classList.toggle("edited", diff);
      if (diff) any = true;
    });
    if ($("savebar").hidden && any) $("savename").value = $("profile").value;
    $("savebar").hidden = !any;
    return any;
  }

  function human(m) {
    m = Number(m);
    return m < 60 ? m + " min" : (Math.floor(m / 60) + "h" + (m % 60 ? " " + (m % 60) + "m" : ""));
  }

  /* interval + a bound = a sequence. The only rule, and the same one the
     bridge enforces - shown here before you commit rather than as an error. */
  function describe(p) {
    var iv = parseFloat(p.interval) || 0,
        du = parseFloat(p.duration) || 0,
        fr = parseInt(p.frames, 10) || 0;
    if (iv > 0 && !du && !fr) return { bad: "An interval needs frames or a duration, or it would run forever." };
    if (du > 0 && !iv) return { bad: "A duration needs an interval — how long between frames?" };
    if (iv > 0) {
      var n = fr || Math.floor(du * 60 / iv), mins = du || Math.round(n * iv / 60);
      return { what: n + " frames over " + human(mins), tl: true,
               text: "A frame every " + iv + "s for " + human(mins) + " — " + n + " frames." };
    }
    var f = fr || 1;
    return { what: f + " frame" + (f === 1 ? "" : "s"),
             text: (f === 1 ? "One" : f) + " frame" + (f === 1 ? "" : "s") +
                   " at " + (p.exposure || "?") + "s, ISO " + (p.iso || "?") + "." };
  }

  function refreshForm() {
    var dirty = edits(), d = describe(formValues()), m = $("msg");
    m.className = "msg";
    if (d.bad) { m.className = "msg warn"; m.textContent = d.bad; $("load").disabled = true; }
    else { m.textContent = d.text; $("load").disabled = false; }

    var loaded = $("profile").value === CURRENT.name && sameParams(formValues(), CURRENT.p);
    var lm = $("loadmsg");
    lm.hidden = loaded || !!d.bad;
    if (!lm.hidden) lm.textContent = "Not the current profile — press Load to shoot with this.";
    $("load").textContent = dirty ? "Load these settings" : "Load as current profile";

    var nm = ($("savename").value || "").trim().toLowerCase().replace(/\s+/g, "-");
    var btn = $("save");
    if (!nm) { btn.textContent = "Save"; btn.disabled = true; }
    else if (PARAMS[nm]) { btn.textContent = "Save to " + nm; btn.disabled = false; }
    else { btn.textContent = "Create " + nm; btn.disabled = false; }
  }

  function refreshCurrent() {
    var d = describe(CURRENT.p);
    var modified = CURRENT.name && PARAMS[CURRENT.name] && !sameParams(CURRENT.p, PARAMS[CURRENT.name]);
    $("s-prof").textContent = (CURRENT.name || "—") + (modified ? " (modified)" : "");
    $("s-what").textContent = d.what || "—";
    $("s-exp").textContent = CURRENT.p.exposure != null ? CURRENT.p.exposure + "s" : "—";
    $("s-iso").textContent = CURRENT.p.iso != null ? CURRENT.p.iso : "—";
    $("fire").textContent = d.tl ? "Start timelapse" : "Capture";
    $("fire").disabled = !!d.bad || CAPTURING;
  }

  $("profile").addEventListener("change", function () { fill(); refreshForm(); });
  FIELD_IDS.forEach(function (f) {
    $(f).addEventListener("input", function () { refreshForm(); });
    $(f).addEventListener("change", function () { refreshForm(); });
  });
  $("savename").addEventListener("input", refreshForm);

  $("reset").addEventListener("click", function () { fill(); refreshForm(); });

  $("save").addEventListener("click", function () {
    var n = ($("savename").value || "").trim().toLowerCase().replace(/\s+/g, "-");
    if (!n) { $("savename").focus(); return; }
    var p = formValues(), isNew = !PARAMS[n];
    /* the prefix is only a label; the parameters decide what it does */
    if (isNew && isSeq(p) && n.indexOf("timelapse-") !== 0) n = "timelapse-" + n;
    var payload = copy(p); payload.name = n;
    pub(isNew ? "command/profile/add" : "command/profile/save", payload);
    PARAMS[n] = p;
    toast((isNew ? "Created " : "Saved to ") + n);
    fillProfileList();
    $("profile").value = n;
    fill(); refreshForm();
    pub("query/profiles");
  });

  $("load").addEventListener("click", function () {
    var name = $("profile").value, p = formValues();
    CURRENT = { name: name, p: p };
    /* If it matches the stored profile, tell the bridge too, so a bare
       /astro capture from Telegram uses the same thing. Edited values are
       sent with the capture itself instead. */
    if (PARAMS[name] && sameParams(p, PARAMS[name])) pub("command/profile/load", { name: name });
    refreshCurrent(); refreshForm();
    toast("Loaded " + name);
  });

  $("fire").addEventListener("click", function () {
    $("err").hidden = true;
    pub("command/capture", CURRENT.p);
    toast("Capture requested");
  });
  $("preview").addEventListener("click", function () {
    $("err").hidden = true;
    pub("command/preview", {});
    toast("Preview requested");
  });
  $("abort").addEventListener("click", function () {
    pub("command/abort", {});
    toast("Abort sent");
  });

  /* ── sessions ──────────────────────────────────────────────────────────────
     Read from nginx's JSON autoindex of /shots, so the console does not need a
     bridge round-trip to know what is on this Pi. Frames are grouped by the
     HHMMSS in their filename, which is the same id the bridge reports. */
  var ARCHIVED = {};   // id -> true, remembered locally this session

  function loadSessions() {
    fetch("/list/?t=" + Date.now())
      .then(function (r) { return r.ok ? r.json() : []; })
      .then(function (list) { render(group(list)); })
      .catch(function () { render([]); });
  }

  function group(list) {
    var by = {};
    list.forEach(function (f) {
      if (!f.name || f.type !== "file") return;
      if (!/\.(arw|fits?|fit)$/i.test(f.name)) return;
      var m = f.name.match(/_(\d{8})_(\d{6})_/);
      if (!m) return;
      var id = m[2];
      if (!by[id]) by[id] = { id: id, date: m[1], frames: 0, mtime: f.mtime };
      by[id].frames++;
    });
    return Object.keys(by).sort().reverse().map(function (k) { return by[k]; });
  }

  function actionBtn(label, fn) {
    var b = document.createElement("button");
    b.textContent = label;
    b.addEventListener("click", fn);
    return b;
  }

  function statusText(s) { return ARCHIVED[s.id] ? "archived" : "on the Pi"; }

  function render(sessions) {
    var pb = $("photo-rows"), tb = $("tl-rows");
    pb.innerHTML = ""; tb.innerHTML = "";
    var photos = sessions.filter(function (s) { return s.frames <= 1; });
    var lapses = sessions.filter(function (s) { return s.frames > 1; });

    if (!photos.length) pb.innerHTML = '<tr><td colspan="3" class="empty">No photos on this Pi.</td></tr>';
    photos.forEach(function (s) {
      var tr = document.createElement("tr");
      tr.innerHTML = '<td class="id">' + s.id + '</td><td class="status">' + statusText(s) + "</td>";
      var td = document.createElement("td"); td.className = "act";
      td.appendChild(actionBtn("Preview", function () { pub("query/archive", { id: s.id + "_1" }); openPreview(s); }));
      if (!ARCHIVED[s.id]) td.appendChild(actionBtn("Archive", function () { archive(s); }));
      tr.appendChild(td); pb.appendChild(tr);
    });

    if (!lapses.length) tb.innerHTML = '<tr><td colspan="4" class="empty">No timelapses on this Pi.</td></tr>';
    lapses.forEach(function (s) {
      var tr = document.createElement("tr");
      tr.innerHTML = '<td class="id">' + s.id + '</td><td>' + s.frames +
                     '</td><td class="status">' + statusText(s) + "</td>";
      var td = document.createElement("td"); td.className = "act";
      td.appendChild(actionBtn("View", function () { window.open("/", "_blank", "noopener"); }));
      if (!ARCHIVED[s.id]) td.appendChild(actionBtn("Archive", function () { archive(s); }));
      tr.appendChild(td); tb.appendChild(tr);
    });
  }

  function openPreview(s) {
    window.open("/", "_blank", "noopener");
  }

  /* Archive is an LXD operation. Publishing query/archive makes the bridge
     emit event/archive, which Node-RED is already listening for. */
  function archive(s) {
    pub("query/archive", { session: s.date });
    ARCHIVED[s.id] = true;
    toast("Archive requested for " + s.id);
    render(group([]));   /* refreshed below from the real listing */
    loadSessions();
  }

  fillLadders();
  setState({});
})();
