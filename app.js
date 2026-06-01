// 因子库前端入口。M1 范围：目录树 + 单因子选中 → 表格 + 净值图 + KPI。

// DuckDB-Wasm runs in a Worker with no notion of the page's "data/" relative path.
// Use absolute URLs (resolved against page origin) for every read_parquet() call.
const DATA_DIR = new URL("data/", document.baseURI).toString();
// Cache-busting：每次加载页面生成一个版本号，避免浏览器缓存旧 parquet 导致
// DuckDB Range 请求拿到错位字节（重跑流水线换数据后必须强制重新下载）。
const V = `?v=${Date.now()}`;
const F_SCORE = DATA_DIR + "factor_score.parquet" + V;
const F_META  = DATA_DIR + "stock_meta.parquet" + V;
const F_BT    = DATA_DIR + "preset_backtest.parquet" + V;
const F_IC    = DATA_DIR + "factor_ic.parquet" + V;

const state = {
  catalog: [],
  activeFactor: null,
  selectedNs: [30],        // 单因子模式：要对比的持仓数集合（至少 1 个）
  scanMetric: "annual",    // 指标-N 曲线的纵轴：annual / sharpe / mdd / nav
  mode: "single",          // single | compare | compose
  compareFactors: [],      // 对比模式：[{code, n}]，每个因子可设不同持仓数
  compareDefaultN: 30,     // 新加入因子的默认持仓数
  // 合成模式：[{code, weight, op:'>='|'<=', thr:number|null}]，thr=null 表示该因子不参与过滤
  composeFactors: [],
  composeN: 30,
  db: null,
};

let navChart = null;
let scanChart = null;
let cmpNavChart = null, cmpIcChart = null, cmpCorrChart = null;
let cpsNavChart = null;

// 多条策略线的配色（按 selectedNs 顺序取）
const STRAT_COLORS = ["#1a4d80", "#e07b39", "#3a9d6e", "#9b59b6", "#c0392b", "#16a085"];

async function init() {
  await loadCatalog();
  renderTree();
  document.getElementById("meta").textContent =
    `${state.catalog.length} 因子可用`;
}

async function loadCatalog() {
  const res = await fetch("data/factor_catalog.json" + V);
  state.catalog = await res.json();
}

function renderTree() {
  const tree = document.getElementById("factor-tree");
  tree.innerHTML = "";
  tree.className = "";

  const byL1 = {};
  for (const f of state.catalog) {
    if (!byL1[f.l1]) byL1[f.l1] = {};
    if (!byL1[f.l1][f.l2]) byL1[f.l1][f.l2] = [];
    byL1[f.l1][f.l2].push(f);
  }

  for (const [l1, l2map] of Object.entries(byL1)) {
    const l1Div = document.createElement("div");
    l1Div.className = "tree-l1";
    l1Div.textContent = "▼ " + l1;
    tree.appendChild(l1Div);

    for (const [l2, factors] of Object.entries(l2map)) {
      const l2Div = document.createElement("div");
      l2Div.className = "tree-l2";
      l2Div.textContent = "▼ " + l2;
      tree.appendChild(l2Div);

      for (const f of factors) {
        const l3Div = document.createElement("div");
        l3Div.className = "tree-l3";
        l3Div.textContent = f.code;
        l3Div.dataset.code = f.code;
        l3Div.title = f.name_cn;
        l3Div.onclick = () => onTreeClick(f.code);
        tree.appendChild(l3Div);
      }
    }
  }
}

let _dbPromise = null;
function ensureDB() {
  // promise 锁：并发调用（快速连点）共享同一次初始化，避免重复 instantiate / 重复建表
  if (!_dbPromise) _dbPromise = _initDB();
  return _dbPromise;
}

async function _initDB() {
  try {
    const duckdb = await import("./vendor/duckdb-browser.mjs");
    const mainModule = new URL("vendor/duckdb-mvp.wasm", document.baseURI).toString();
    const workerUrl = new URL("vendor/duckdb-browser-mvp.worker.js", document.baseURI).toString();
    const worker = new Worker(workerUrl);
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(mainModule);
    state.db = await db.connect();
    console.log("DuckDB-Wasm ready, loading tables…");

    // 串行加载核心表（并发会让单线程/多线程 server 偶发 ERR_EMPTY_RESPONSE）。
    // factor_score 只 load 最新截面（前端只用这个，避免 25MB 长表全 load）。
    const t0 = performance.now();
    // factor_score.parquet 已是最新单截面（06_export 已瘦身），直接读，不再 WHERE=MAX 二次扫文件
    await state.db.query(`CREATE TABLE factor_score AS SELECT * FROM read_parquet('${F_SCORE}')`);
    await state.db.query(`CREATE TABLE stock_meta AS SELECT * FROM read_parquet('${F_META}')`);
    await state.db.query(`CREATE TABLE preset_backtest AS SELECT * FROM read_parquet('${F_BT}')`);
    await state.db.query(`CREATE TABLE factor_ic AS SELECT * FROM read_parquet('${F_IC}')`);
    console.log(`核心表加载 ${(performance.now() - t0).toFixed(0)}ms`);

    // 可选数据（串行）：没准备好就建空表，前端 LEFT JOIN 自动出 NULL
    state.hasDescriptors = await tryLoadOptional("stock_descriptors", `
      CREATE TABLE stock_descriptors AS
      SELECT * FROM read_parquet('${DATA_DIR}stock_descriptors.parquet${V}')
    `, `
      CREATE TABLE stock_descriptors (
        stock_code VARCHAR, industry_sw1 VARCHAR, industry_sw2 VARCHAR,
        market_cap DOUBLE, pe DOUBLE, pb DOUBLE, avg_amount DOUBLE
      )
    `);
    state.hasBenchmarks = await tryLoadOptional("benchmarks", `
      CREATE TABLE benchmarks AS
      SELECT * FROM read_parquet('${DATA_DIR}benchmarks.parquet${V}')
    `, `
      CREATE TABLE benchmarks (
        trade_date DATE, index_code VARCHAR, nav DOUBLE
      )
    `);
    state.hasCorr = await tryLoadOptional("factor_corr", `
      CREATE TABLE factor_corr AS
      SELECT * FROM read_parquet('${DATA_DIR}factor_corr.parquet${V}')
    `, `
      CREATE TABLE factor_corr (factor_a VARCHAR, factor_b VARCHAR, corr DOUBLE)
    `);
    console.log(`Optional: descriptors=${state.hasDescriptors}, benchmarks=${state.hasBenchmarks}, corr=${state.hasCorr}`);

    return state.db;
  } catch (err) {
    console.error("DuckDB init failed:", err);
    showError(`DuckDB 初始化失败: ${err.message || err}`);
    _dbPromise = null;   // 允许重试
    throw err;
  }
}

// 合成专用大表（25MB 全量得分 + 6MB 月收益）懒加载：只在合成模式首次用时串行加载，
// 避免单因子/对比模式也并发拖大文件导致 ERR_EMPTY_RESPONSE。
let _composePromise = null;
function ensureComposeData() {
  // promise 锁：并发调用共享同一次加载，避免重复 CREATE TABLE 竞态
  if (!_composePromise) {
    _composePromise = (async () => {
      // factor_score_full（46 因子全历史，65MB）用 VIEW 直挂远程 parquet，不整表物化。
      // parquet 已按 factor_code 聚簇排序（见 scripts/09），合成只选 2~5 个因子时，
      // DuckDB 借 row-group 统计裁剪 + HTTP Range 只读相关字节，避免首次下载整表。
      // monthly_return 较小（~6MB）且回测每月都要 join 全集，物化成表更快。
      const ok1 = await tryLoadOptional("factor_score_full", `
        CREATE VIEW factor_score_full AS SELECT * FROM read_parquet('${DATA_DIR}factor_score_full.parquet${V}')
      `, `CREATE TABLE factor_score_full (trade_date DATE, stock_code VARCHAR, factor_code VARCHAR, score DOUBLE)`);
      const ok2 = await tryLoadOptional("monthly_return", `
        CREATE TABLE monthly_return AS SELECT * FROM read_parquet('${DATA_DIR}monthly_return.parquet${V}')
      `, `CREATE TABLE monthly_return (trade_date DATE, stock_code VARCHAR, fwd_return DOUBLE)`);
      state.hasComposeData = ok1 && ok2;
      console.log(`Compose data loaded: ${state.hasComposeData}`);
      return state.hasComposeData;
    })();
  }
  return _composePromise;
}

async function tryLoadOptional(tableName, loadSql, emptySql) {
  try {
    await state.db.query(loadSql);
    return true;
  } catch (err) {
    console.warn(`optional data ${tableName} not available, creating empty table:`, err.message);
    await state.db.query(emptySql);
    return false;
  }
}

function showError(msg) {
  const detail = document.getElementById("factor-detail");
  detail.innerHTML = `<h3 style="color:#c00">错误</h3><pre style="color:#c00;white-space:pre-wrap;font-size:11px">${msg}</pre>`;
}

async function selectFactor(code) {
  state.activeFactor = code;
  document.querySelectorAll(".tree-l3").forEach(el => {
    el.classList.toggle("active", el.dataset.code === code);
  });
  const meta = state.catalog.find(f => f.code === code);
  try {
    const tAll = performance.now();
    await ensureDB();
    renderFactorDetail(meta);
    const tQ = performance.now();
    await Promise.all([
      (async () => { const t = performance.now(); await renderTopStocks(code); console.log(`  top table: ${(performance.now()-t).toFixed(0)}ms`); })(),
      (async () => { const t = performance.now(); await renderNavChart(code); console.log(`  nav chart: ${(performance.now()-t).toFixed(0)}ms`); })(),
      (async () => { const t = performance.now(); await renderNScan(code); console.log(`  N-scan:    ${(performance.now()-t).toFixed(0)}ms`); })(),
      (async () => { const t = performance.now(); await renderKpiTable(code); console.log(`  kpi: ${(performance.now()-t).toFixed(0)}ms`); })(),
    ]);
    console.log(`selectFactor(${code}, N=[${state.selectedNs}]) total ${(performance.now()-tAll).toFixed(0)}ms (queries ${(performance.now()-tQ).toFixed(0)}ms)`);
  } catch (err) {
    console.error("selectFactor failed:", err);
    showError(`选择因子 ${code} 失败: ${err.message || err}\n\n${err.stack || ""}`);
  }
}

const PRESET_NS = Array.from({ length: 100 }, (_, i) => i + 1);  // 1..100 全档位
const QUICK_NS = [5, 10, 20, 30, 50, 100];                       // UI 快捷按钮

function maxN() { return Math.max(...state.selectedNs); }

function toggleN(n) {
  const i = state.selectedNs.indexOf(n);
  if (i >= 0) {
    if (state.selectedNs.length === 1) return;   // 至少保留 1 个
    state.selectedNs.splice(i, 1);
  } else {
    state.selectedNs.push(n);
  }
  state.selectedNs.sort((a, b) => a - b);
  selectFactor(state.activeFactor);
}

function renderFactorDetail(meta) {
  const dirArrow = meta.direction === 1 ? "↑（越高越好）" : "↓（越低越好）";
  const presetTags = QUICK_NS.map(n =>
    `<button class="topn-btn${state.selectedNs.includes(n) ? ' active' : ''}" data-n="${n}">${n}</button>`
  ).join("");
  // 已选 N 的 chips（带 × 移除）
  const chips = state.selectedNs.map(n =>
    `<span class="n-chip" data-n="${n}">top${n} ${state.selectedNs.length > 1 ? '×' : ''}</span>`
  ).join("");
  const formulaBlock = meta.formula ? `
    <div style="margin-top:8px">
      <div class="label" style="color:#888;font-size:11px">计算公式</div>
      <pre style="background:#fff;border:1px solid #e0e0e0;border-radius:4px;padding:8px 10px;
                  font-size:12px;line-height:1.5;white-space:pre-wrap;margin-top:3px;color:#333">${meta.formula}</pre>
    </div>` : "";
  const sourceBlock = meta.wind_source ? `
    <div style="margin-top:8px">
      <div class="label" style="color:#888;font-size:11px">数据来源（Wind 表.字段）</div>
      <p style="font-size:12px;color:#444;margin-top:3px">${meta.wind_source}</p>
    </div>` : "";
  document.getElementById("factor-detail").innerHTML = `
    <h3>${meta.code}　·　${meta.name_cn}</h3>
    <p><b>${meta.l1} → ${meta.l2}</b>　方向：${dirArrow}</p>
    <p>${meta.description}</p>
    ${formulaBlock}
    ${sourceBlock}
    <div style="margin-top:10px;display:flex;align-items:center;gap:8px;flex-wrap:wrap">
      <span style="color:#666;font-size:11px">选股数（可多选对比）：</span>
      <div>${presetTags}</div>
      <span style="color:#666;font-size:11px">或加入</span>
      <input id="topn-input" type="number" min="1" max="100" placeholder="1-100"
             style="width:64px;padding:3px 6px;border:1px solid #ccc;border-radius:3px;font-size:12px" />
      <button id="topn-add" class="topn-btn">+ 加入</button>
    </div>
    <div style="margin-top:6px;display:flex;align-items:center;gap:6px;flex-wrap:wrap">
      <span style="color:#666;font-size:11px">已选：</span>${chips}
    </div>
    <p style="color:#666;font-size:11px;margin-top:8px">
      下方股票表显示 <b>top${maxN()}</b>（小 N 是其子集）；净值图 / 指标表叠加对比所选各 N。
      口径：每月末按 <b>${meta.code}</b> z-score 排序选非 ST 股等权持有，扣 0.2% 双边成本，2020-01 ~ 2025-12。
    </p>
  `;
  document.querySelectorAll(".topn-btn[data-n]").forEach(btn => {
    btn.onclick = () => toggleN(parseInt(btn.dataset.n, 10));
  });
  document.querySelectorAll(".n-chip").forEach(chip => {
    chip.onclick = () => toggleN(parseInt(chip.dataset.n, 10));
  });
  const inp = document.getElementById("topn-input");
  const addN = () => {
    const n = parseInt(inp.value, 10);
    if (!Number.isFinite(n) || n < 1 || n > 100) { inp.value = ""; return; }
    if (!state.selectedNs.includes(n)) toggleN(n);
    else inp.value = "";
  };
  document.getElementById("topn-add").onclick = addN;
  inp.addEventListener("keydown", (e) => { if (e.key === "Enter") addN(); });
}

async function renderTopStocks(code) {
  const N = maxN();
  const target = document.getElementById("top-stocks");
  target.innerHTML = `<h3>${code} · Top ${N} 股票（最新月末截面）</h3><div class="loading">查询中…</div>`;

  // 用子查询取最新截面日，避免 JS<->DuckDB 日期类型转换问题
  // LEFT JOIN stock_descriptors（可能为空）：行业/市值/PE/PB/成交量
  const res = await state.db.query(`
    WITH latest AS (
      SELECT MAX(trade_date) AS d FROM factor_score WHERE factor_code = '${code}'
    )
    SELECT
      s.stock_code, m.name, s.score, s.raw_value,
      CAST(s.trade_date AS VARCHAR) AS dt,
      d.industry_sw1, d.industry_sw2,
      d.market_cap, d.pe, d.pb, d.avg_amount
    FROM factor_score s
    LEFT JOIN stock_meta m USING(stock_code)
    LEFT JOIN stock_descriptors d USING(stock_code)
    WHERE s.factor_code = '${code}'
      AND s.trade_date = (SELECT d FROM latest)
      AND s.score IS NOT NULL
      AND COALESCE(m.is_st, FALSE) = FALSE
      AND COALESCE(m.is_active_latest, FALSE) = TRUE
    ORDER BY s.score DESC
    LIMIT ${N}
  `);

  const rows = res.toArray();
  if (rows.length === 0) {
    target.innerHTML = `<h3>${code} · Top ${N} 股票</h3><div class="empty">无数据（该因子该截面无有效得分）</div>`;
    return;
  }

  const dt = rows[0].dt;
  const descNote = state.hasDescriptors ? "" :
    " <span style='color:#aaa;font-size:11px'>(行业/市值/PE/PB/成交额待数据)</span>";
  let html = `<h3>${code} · Top ${N} 股票（截面日 ${dt}，按 z-score 降序）${descNote}</h3>
    <p style="color:#888;font-size:11px;margin:-4px 0 8px 0">
      指标口径：得分/原始值基于因子截面 ${dt}；申万行业 / 市值 / PE / PB 为 ${dt} 当日快照；
      近一年日均成交额为截至 ${dt} 往前 252 个交易日的日均。
    </p>
    <table class="stock-table">
      <thead><tr>
        <th>#</th><th>代码</th><th>名称</th>
        <th>申万一级</th><th>申万二级</th>
        <th>市值(亿)</th><th>PE</th><th>PB</th><th>近一年日均成交额(亿)</th>
        <th>得分</th><th>原始值</th>
      </tr></thead>
      <tbody>`;
  const fmt = (v, dp = 2) => (v === null || v === undefined ? "—" : Number(v).toFixed(dp));
  const fmtMV = (v) => (v === null || v === undefined ? "—" : (Number(v) / 1e4).toFixed(0));  // 万元 → 亿元
  const fmtAmt = (v) => (v === null || v === undefined ? "—" : Number(v).toFixed(2));  // 已是亿元
  rows.forEach((r, i) => {
    html += `<tr>
      <td>${i + 1}</td>
      <td>${r.stock_code}</td>
      <td>${r.name || ""}</td>
      <td>${r.industry_sw1 || "—"}</td>
      <td>${r.industry_sw2 || "—"}</td>
      <td>${fmtMV(r.market_cap)}</td>
      <td>${fmt(r.pe, 1)}</td>
      <td>${fmt(r.pb, 2)}</td>
      <td>${fmtAmt(r.avg_amount)}</td>
      <td>${fmt(r.score, 3)}</td>
      <td>${r.raw_value !== null && r.raw_value !== undefined ? Number(r.raw_value).toFixed(4) : "—"}</td>
    </tr>`;
  });
  html += "</tbody></table>";
  target.innerHTML = html;
}

async function renderNavChart(code) {
  const ns = state.selectedNs;
  document.getElementById("nav-title").textContent =
    `${code} · 组合净值对比 top-[${ns.join(", ")}]（起点=1.0；2020-01~2025-12，月末等权，0.2%双边成本）`;

  const chartDiv = document.getElementById("nav-chart");
  if (navChart) { navChart.dispose(); navChart = null; }
  chartDiv.innerHTML = "";

  // 一次查出所选各 N 的净值序列
  const inList = ns.join(",");
  const res = await state.db.query(`
    SELECT top_n, strftime(trade_date, '%Y-%m') AS dt, nav
    FROM preset_backtest
    WHERE factor_code = '${code}' AND top_n IN (${inList})
    ORDER BY top_n, trade_date
  `);
  const byN = {};
  for (const r of res.toArray()) {
    if (!byN[r.top_n]) byN[r.top_n] = { dt: [], nav: [] };
    byN[r.top_n].dt.push(r.dt); byN[r.top_n].nav.push(r.nav);
  }
  // x 轴用第一个 N 的月份（各 N 月份一致）
  const x = (byN[ns[0]] || { dt: [] }).dt;

  const series = [];
  ns.forEach((n, i) => {
    const s = byN[n];
    if (!s) return;
    const base = s.nav[0] || 1;
    series.push({
      name: `top${n}`,
      type: "line",
      data: s.nav.map(v => v / base),   // rebase 到 1.0
      symbol: "none",
      color: STRAT_COLORS[i % STRAT_COLORS.length],   // legend 标记与线同色
      lineStyle: { width: 2 },
    });
  });

  // 基准：单 N 时画全部 3 条；多 N 对比时只留沪深300 一条灰线作参照（避免太挤）
  if (state.hasBenchmarks && x.length) {
    const bmRes = await state.db.query(`
      SELECT index_code, strftime(trade_date, '%Y-%m') AS dt, nav
      FROM benchmarks
      WHERE strftime(trade_date, '%Y-%m') >= '${x[0]}'
        AND strftime(trade_date, '%Y-%m') <= '${x[x.length - 1]}'
      ORDER BY index_code, trade_date
    `);
    const byIndex = {};
    for (const r of bmRes.toArray()) {
      if (!byIndex[r.index_code]) byIndex[r.index_code] = {};
      byIndex[r.index_code][r.dt] = r.nav;
    }
    const colors = { "HS300": "#c14545", "CSI800": "#6e9a4f", "CSI500": "#c89c2b" };
    const cnNames = { "HS300": "沪深300", "CSI800": "中证800", "CSI500": "中证500" };
    const wantIdx = ["HS300", "CSI800", "CSI500"];
    for (const idxCode of wantIdx) {
      const monthMap = byIndex[idxCode];
      if (!monthMap) continue;
      const aligned = x.map(m => (m in monthMap ? monthMap[m] : null));
      const b = aligned.find(v => v !== null);
      const rebased = b ? aligned.map(v => (v === null ? null : v / b)) : aligned;
      series.push({
        name: `${cnNames[idxCode] || idxCode}(基准)`,
        type: "line", data: rebased, symbol: "none", connectNulls: true,
        color: colors[idxCode] || "#888",
        lineStyle: { width: 1.2, type: "dashed" },
      });
    }
  }

  navChart = echarts.init(chartDiv);
  navChart.setOption({
    grid: { left: 50, right: 20, top: 30, bottom: 30 },
    tooltip: { trigger: "axis" },
    legend: { top: 0, textStyle: { fontSize: 11 }, itemWidth: 32 },
    xAxis: { type: "category", data: x, axisLabel: { fontSize: 10 } },
    yAxis: { type: "value", scale: true },
    series,
  });
}

// 从月度收益/净值序列算各项指标
function computeMetrics(rets, navs) {
  if (navs.length < 2) return null;
  const n = rets.length;
  const totalRet = navs[navs.length - 1] / navs[0] - 1;
  const annual = Math.pow(1 + totalRet, 12 / n) - 1;
  const mean = rets.reduce((s, v) => s + v, 0) / n;
  const std = Math.sqrt(rets.reduce((s, v) => s + (v - mean) ** 2, 0) / (n - 1));
  const sharpe = std > 0 ? mean / std * Math.sqrt(12) : 0;
  let peak = navs[0], mdd = 0;
  for (const v of navs) { if (v > peak) peak = v; const dd = v / peak - 1; if (dd < mdd) mdd = dd; }
  const winRate = rets.filter(r => r > 0).length / n;
  const navEnd = navs[navs.length - 1] / navs[0];
  const vol = std * Math.sqrt(12);   // 年化波动率
  return { annual, sharpe, mdd, winRate, navEnd, vol };
}

// 基准年化（用于超额计算），按因子回测区间对齐
let _benchAnnualCache = null;
async function benchAnnuals() {
  if (_benchAnnualCache) return _benchAnnualCache;
  const out = {};
  if (state.hasBenchmarks) {
    const r = await state.db.query(`
      SELECT index_code, nav FROM benchmarks
      WHERE index_code IN ('HS300','CSI800')
        AND trade_date BETWEEN (SELECT MIN(trade_date) FROM preset_backtest)
                           AND (SELECT MAX(trade_date) FROM preset_backtest)
      ORDER BY index_code, trade_date
    `);
    const g = {};
    for (const row of r.toArray()) { (g[row.index_code] ||= []).push(row.nav); }
    for (const [k, arr] of Object.entries(g)) {
      if (arr.length >= 2) out[k] = Math.pow(arr[arr.length - 1] / arr[0], 12 / arr.length) - 1;
    }
  }
  _benchAnnualCache = out;
  return out;
}

async function renderKpiTable(code) {
  const target = document.getElementById("kpi");
  // 一次查所选各 N 的月收益
  const res = await state.db.query(`
    SELECT top_n, port_ret, nav FROM preset_backtest
    WHERE factor_code = '${code}' AND top_n IN (${state.selectedNs.join(",")})
    ORDER BY top_n, trade_date
  `);
  const byN = {};
  for (const r of res.toArray()) {
    if (!byN[r.top_n]) byN[r.top_n] = { rets: [], navs: [] };
    if (r.port_ret !== null) byN[r.top_n].rets.push(r.port_ret);
    if (r.nav !== null) byN[r.top_n].navs.push(r.nav);
  }
  const ba = await benchAnnuals();

  // 因子级 IC_IR（与 N 无关）
  const icRes = await state.db.query(`
    SELECT ic_ir_12m FROM factor_ic
    WHERE factor_code = '${code}' AND ic_ir_12m IS NOT NULL AND NOT ISNAN(ic_ir_12m)
    ORDER BY month DESC LIMIT 1
  `);
  const icRow = icRes.toArray()[0];
  const icir = icRow && Number.isFinite(icRow.ic_ir_12m) ? Number(icRow.ic_ir_12m).toFixed(2) : "—";

  const pct = (v) => (v * 100).toFixed(1) + "%";
  const signed = (v) => (v >= 0 ? "+" : "") + (v * 100).toFixed(1) + "%";
  let rows = "";
  for (const n of state.selectedNs) {
    const d = byN[n];
    const m = d ? computeMetrics(d.rets, d.navs) : null;
    if (!m) { rows += `<tr><td>top${n}</td><td colspan="6">无数据</td></tr>`; continue; }
    const ex300 = ("HS300" in ba) ? signed(m.annual - ba.HS300) : "—";
    const ex800 = ("CSI800" in ba) ? signed(m.annual - ba.CSI800) : "—";
    rows += `<tr>
      <td>top${n}</td>
      <td>${pct(m.annual)}</td>
      <td>${m.sharpe.toFixed(2)}</td>
      <td>${pct(m.mdd)}</td>
      <td>${(m.winRate * 100).toFixed(0)}%</td>
      <td>${ex300}</td>
      <td>${ex800}</td>
    </tr>`;
  }

  // 基准行：从月末 nav 序列算绝对指标（超额列对基准自身无意义，留 —）
  if (state.hasBenchmarks) {
    const cnNames = { "HS300": "沪深300", "CSI800": "中证800", "CSI500": "中证500" };
    const bRes = await state.db.query(`
      SELECT index_code, nav FROM benchmarks
      WHERE index_code IN ('HS300','CSI800','CSI500')
        AND trade_date BETWEEN (SELECT MIN(trade_date) FROM preset_backtest)
                           AND (SELECT MAX(trade_date) FROM preset_backtest)
      ORDER BY index_code, trade_date
    `);
    const bg = {};
    for (const r of bRes.toArray()) { (bg[r.index_code] ||= []).push(r.nav); }
    for (const idx of ["HS300", "CSI800", "CSI500"]) {
      const navs = bg[idx];
      if (!navs || navs.length < 2) continue;
      const rets = navs.slice(1).map((v, i) => v / navs[i] - 1);
      const m = computeMetrics(rets, navs);
      rows += `<tr style="color:#888;border-top:2px solid #ddd">
        <td style="color:#888">${cnNames[idx]}</td>
        <td>${pct(m.annual)}</td>
        <td>${m.sharpe.toFixed(2)}</td>
        <td>${pct(m.mdd)}</td>
        <td>${(m.winRate * 100).toFixed(0)}%</td>
        <td>—</td><td>—</td>
      </tr>`;
    }
  }

  target.innerHTML = `
    <table class="kpi-table">
      <thead><tr>
        <th>组合 / 基准</th><th>年化收益</th><th>夏普</th><th>最大回撤</th>
        <th>月度胜率</th><th>超额 vs 300</th><th>超额 vs 800</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>
    <p style="color:#888;font-size:11px;margin-top:6px">因子 12 月 IC_IR：${icir}（与持仓数无关）</p>
  `;
}

// 指标-N 曲线：横轴持仓数 1-100，纵轴当前选定指标
async function renderNScan(code) {
  const metricLabels = { annual: "年化收益", sharpe: "夏普", mdd: "最大回撤", nav: "期末净值" };
  document.getElementById("scan-title").textContent =
    `${code} · ${metricLabels[state.scanMetric]} vs 持仓数（top-1 ~ top-100 全扫描）`;
  const chartDiv = document.getElementById("scan-chart");
  if (scanChart) { scanChart.dispose(); scanChart = null; }
  chartDiv.innerHTML = "";

  const res = await state.db.query(`
    SELECT top_n, port_ret, nav FROM preset_backtest
    WHERE factor_code = '${code}'
    ORDER BY top_n, trade_date
  `);
  const byN = {};
  for (const r of res.toArray()) {
    if (!byN[r.top_n]) byN[r.top_n] = { rets: [], navs: [] };
    if (r.port_ret !== null) byN[r.top_n].rets.push(r.port_ret);
    if (r.nav !== null) byN[r.top_n].navs.push(r.nav);
  }
  const xs = Object.keys(byN).map(Number).sort((a, b) => a - b);
  const ys = xs.map(n => {
    const m = computeMetrics(byN[n].rets, byN[n].navs);
    if (!m) return null;
    if (state.scanMetric === "annual") return +(m.annual * 100).toFixed(2);
    if (state.scanMetric === "sharpe") return +m.sharpe.toFixed(3);
    if (state.scanMetric === "mdd") return +(m.mdd * 100).toFixed(2);
    return +m.navEnd.toFixed(3);
  });
  // 标出当前所选的 N
  const marks = state.selectedNs.map(n => {
    const idx = xs.indexOf(n);
    return idx >= 0 ? { xAxis: n, yAxis: ys[idx] } : null;
  }).filter(Boolean);

  scanChart = echarts.init(chartDiv);
  scanChart.setOption({
    grid: { left: 55, right: 20, top: 20, bottom: 36 },
    tooltip: { trigger: "axis", formatter: p => `top${p[0].axisValue}<br/>${metricLabels[state.scanMetric]}: ${p[0].data}` },
    xAxis: { type: "category", data: xs, name: "持仓数 N", nameLocation: "middle", nameGap: 24, axisLabel: { fontSize: 10 } },
    yAxis: { type: "value", scale: true },
    series: [{
      type: "line", data: ys, symbol: "none", smooth: true,
      lineStyle: { color: "#1a4d80", width: 1.8 },
      markPoint: { data: marks.map(m => ({ coord: [String(m.xAxis), m.yAxis] })), symbol: "pin", symbolSize: 36,
                   itemStyle: { color: "#e07b39" }, label: { fontSize: 9, formatter: p => "N=" + p.data.coord[0] } },
    }],
  });
}

// ===================== 模式切换 + 多因子对比 =====================

function onTreeClick(code) {
  if (state.mode === "single") selectFactor(code);
  else if (state.mode === "compare") addCompareFactor(code);   // 对比：每次点击加一行（允许重复）
  else toggleComposeFactor(code);                               // 合成：toggle
}

function cmpHas(code) { return state.compareFactors.some(f => f.code === code); }
function cpsHas(code) { return state.composeFactors.some(f => f.code === code); }

function updateTreeHighlight() {
  document.querySelectorAll(".tree-l3").forEach(el => {
    const c = el.dataset.code;
    let on = false;
    if (state.mode === "single") on = (c === state.activeFactor);
    else if (state.mode === "compare") on = cmpHas(c);
    else on = cpsHas(c);
    el.classList.toggle("active", on);
  });
}

function switchMode(mode) {
  state.mode = mode;
  document.querySelectorAll(".mode-btn").forEach(b =>
    b.classList.toggle("active", b.dataset.mode === mode));
  document.getElementById("single-view").style.display = mode === "single" ? "flex" : "none";
  document.getElementById("compare-view").style.display = mode === "compare" ? "flex" : "none";
  document.getElementById("compose-view").style.display = mode === "compose" ? "flex" : "none";
  document.getElementById("ranking-view").style.display = mode === "ranking" ? "flex" : "none";
  updateTreeHighlight();
  if (mode === "compare") renderCompare();
  if (mode === "compose") renderCompose();
  if (mode === "ranking") renderRanking();
}

function addCompareFactor(code) {
  state.compareFactors.push({ code, n: state.compareDefaultN });
  updateTreeHighlight();
  renderCompare();
}

function removeCompareAt(i) {
  state.compareFactors.splice(i, 1);
  updateTreeHighlight();
  renderCompare();
}

// 渲染每个已选因子 + 各自持仓数选择器
function renderCmpControls() {
  const box = document.getElementById("cmp-controls");
  if (state.compareFactors.length === 0) {
    box.innerHTML = `<div class="empty">未选因子</div>`;
    return;
  }
  // 用 index 标识每一行（同因子可重复，不能用 code）
  box.innerHTML = state.compareFactors.map((f, i) => `
    <span class="cmp-frow" style="display:inline-flex;align-items:center;gap:4px;margin:0 10px 6px 0">
      <span style="width:10px;height:10px;border-radius:50%;background:${STRAT_COLORS[i % STRAT_COLORS.length]};display:inline-block"></span>
      <b style="font-size:12px">${f.code}</b>
      <span style="color:#888;font-size:11px">top</span>
      <input class="cmp-n-input" data-idx="${i}" type="number" min="1" max="100" value="${f.n}"
             style="width:52px;padding:2px 4px;border:1px solid #ccc;border-radius:3px;font-size:12px" />
      <span class="cmp-remove" data-idx="${i}"
            style="cursor:pointer;color:#c14545;font-size:13px;padding:0 2px">×</span>
    </span>
  `).join("");
  box.querySelectorAll(".cmp-n-input").forEach(inp => {
    inp.addEventListener("change", () => {
      const idx = parseInt(inp.dataset.idx, 10);
      const f = state.compareFactors[idx];
      if (!f) return;
      const n = parseInt(inp.value, 10);
      if (!Number.isFinite(n) || n < 1 || n > 100) { inp.value = f.n; return; }
      f.n = n;
      renderCmpTable(); renderCmpNav();   // IC/相关性与 N 无关，不重画
    });
  });
  box.querySelectorAll(".cmp-remove").forEach(x => {
    x.onclick = () => removeCompareAt(parseInt(x.dataset.idx, 10));
  });
}

async function renderCompare() {
  const sel = state.compareFactors;
  document.getElementById("cmp-selected").textContent = sel.length ? `（已选 ${sel.length} 个）` : "";
  renderCmpControls();
  try {
    await ensureDB();   // compare 路径也保证 DuckDB 已初始化
    if (sel.length === 0) {
      document.getElementById("cmp-table").innerHTML = `<div class="empty">从左侧选 1 个以上因子开始对比</div>`;
      return;
    }
    await Promise.all([renderCmpTable(), renderCmpNav(), renderCmpIc(), renderCmpCorr()]);
  } catch (err) {
    console.error("renderCompare failed:", err);
    document.getElementById("cmp-table").innerHTML =
      `<pre style="color:#c00;white-space:pre-wrap;font-size:11px">对比渲染失败：${err.message || err}\n\n${err.stack || ""}</pre>`;
  }
}

// 每因子用各自 n 拼 OR 条件： (factor_code='A' AND top_n=10) OR ...
function cmpPairCond() {
  return state.compareFactors.map(f => `(factor_code='${f.code}' AND top_n=${f.n})`).join(" OR ");
}

async function renderCmpTable() {
  const target = document.getElementById("cmp-table");
  document.getElementById("cmp-table-title").textContent = `因子指标对比表（各因子可设不同持仓数）`;
  if (state.compareFactors.length === 0) {
    target.innerHTML = `<div class="empty">从左侧选 1 个以上因子开始对比</div>`;
    return;
  }
  const inList = [...new Set(state.compareFactors.map(f => `'${f.code}'`))].join(",");
  // 各因子用各自 n 取月收益；按 (code,n) 分组（同因子可重复用不同 N）
  const res = await state.db.query(`
    SELECT factor_code, top_n, port_ret, nav FROM preset_backtest
    WHERE ${cmpPairCond()}
    ORDER BY factor_code, top_n, trade_date
  `);
  const byKey = {};
  for (const r of res.toArray()) {
    const k = `${r.factor_code}_${r.top_n}`;
    if (!byKey[k]) byKey[k] = { rets: [], navs: [] };
    if (r.port_ret !== null) byKey[k].rets.push(r.port_ret);
    if (r.nav !== null) byKey[k].navs.push(r.nav);
  }
  // 各因子 IC 统计（与 N 无关）
  const icRes = await state.db.query(`
    SELECT factor_code,
           AVG(ic) AS ic_mean, AVG(rank_ic) AS rankic_mean
    FROM factor_ic WHERE factor_code IN (${inList}) AND NOT ISNAN(ic)
    GROUP BY factor_code
  `);
  const icMap = {};
  for (const r of icRes.toArray()) icMap[r.factor_code] = r;
  const icirRes = await state.db.query(`
    SELECT factor_code, ic_ir_12m FROM factor_ic
    WHERE factor_code IN (${inList}) AND ic_ir_12m IS NOT NULL AND NOT ISNAN(ic_ir_12m)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY factor_code ORDER BY month DESC) = 1
  `);
  const icirMap = {};
  for (const r of icirRes.toArray()) icirMap[r.factor_code] = r.ic_ir_12m;

  const ba = await benchAnnuals();
  const pct = (v) => (v * 100).toFixed(1) + "%";
  const num = (v, d = 2) => (v === null || v === undefined || !Number.isFinite(v) ? "—" : Number(v).toFixed(d));

  let rows = "";
  for (const f of state.compareFactors) {
    const code = f.code;
    const d = byKey[`${code}_${f.n}`];
    const m = d ? computeMetrics(d.rets, d.navs) : null;
    const ic = icMap[code] || {};
    const label = `${code} <span style="color:#888;font-weight:400">top${f.n}</span>`;
    if (!m) { rows += `<tr><td>${label}</td><td colspan="7">无数据</td></tr>`; continue; }
    const ex300 = ("HS300" in ba) ? ((m.annual - ba.HS300) * 100).toFixed(1) + "%" : "—";
    rows += `<tr>
      <td>${label}</td>
      <td>${pct(m.annual)}</td>
      <td>${m.sharpe.toFixed(2)}</td>
      <td>${pct(m.mdd)}</td>
      <td>${(m.winRate * 100).toFixed(0)}%</td>
      <td>${ex300}</td>
      <td>${num(ic.ic_mean, 3)}</td>
      <td>${num(icirMap[code], 2)}</td>
    </tr>`;
  }
  // 基准行
  if (state.hasBenchmarks) {
    const cn = { HS300: "沪深300", CSI800: "中证800", CSI500: "中证500" };
    const bRes = await state.db.query(`
      SELECT index_code, nav FROM benchmarks
      WHERE index_code IN ('HS300','CSI800','CSI500')
        AND trade_date BETWEEN (SELECT MIN(trade_date) FROM preset_backtest)
                           AND (SELECT MAX(trade_date) FROM preset_backtest)
      ORDER BY index_code, trade_date
    `);
    const bg = {};
    for (const r of bRes.toArray()) (bg[r.index_code] ||= []).push(r.nav);
    for (const idx of ["HS300", "CSI800", "CSI500"]) {
      const navs = bg[idx]; if (!navs || navs.length < 2) continue;
      const rets = navs.slice(1).map((v, i) => v / navs[i] - 1);
      const m = computeMetrics(rets, navs);
      rows += `<tr style="color:#888;border-top:2px solid #ddd">
        <td style="color:#888">${cn[idx]}</td>
        <td>${pct(m.annual)}</td><td>${m.sharpe.toFixed(2)}</td><td>${pct(m.mdd)}</td>
        <td>${(m.winRate * 100).toFixed(0)}%</td><td>—</td><td>—</td><td>—</td>
      </tr>`;
    }
  }
  target.innerHTML = `
    <table class="kpi-table">
      <thead><tr>
        <th>因子 / 基准</th><th>年化收益</th><th>夏普</th><th>最大回撤</th>
        <th>月度胜率</th><th>超额 vs 300</th><th>IC 均值</th><th>IC_IR</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

async function renderCmpNav() {
  document.getElementById("cmp-nav-title").textContent = `组合净值叠加（各因子按各自持仓数，起点=1.0）`;
  const div = document.getElementById("cmp-nav-chart");
  if (cmpNavChart) { cmpNavChart.dispose(); cmpNavChart = null; }
  div.innerHTML = "";
  if (state.compareFactors.length === 0) { div.innerHTML = `<div class="empty">选因子后显示</div>`; return; }

  const res = await state.db.query(`
    SELECT factor_code, top_n, strftime(trade_date,'%Y-%m') AS dt, nav
    FROM preset_backtest WHERE ${cmpPairCond()}
    ORDER BY factor_code, top_n, trade_date
  `);
  const byKey = {};
  for (const r of res.toArray()) { const k = `${r.factor_code}_${r.top_n}`; (byKey[k] ||= { dt: [], nav: [] }); byKey[k].dt.push(r.dt); byKey[k].nav.push(r.nav); }
  const first = state.compareFactors[0];
  const x = (byKey[`${first.code}_${first.n}`] || { dt: [] }).dt;
  const series = state.compareFactors.map((f, i) => {
    const s = byKey[`${f.code}_${f.n}`]; if (!s) return null;
    const base = s.nav[0] || 1;
    return { name: `${f.code} top${f.n}`, type: "line", symbol: "none",
             data: s.nav.map(v => v / base),
             color: STRAT_COLORS[i % STRAT_COLORS.length],
             lineStyle: { width: 2 } };
  }).filter(Boolean);

  if (state.hasBenchmarks && x.length) {
    const bmRes = await state.db.query(`
      SELECT index_code, strftime(trade_date,'%Y-%m') AS dt, nav FROM benchmarks
      WHERE strftime(trade_date,'%Y-%m') >= '${x[0]}' AND strftime(trade_date,'%Y-%m') <= '${x[x.length-1]}'
      ORDER BY index_code, trade_date
    `);
    const byIdx = {};
    for (const r of bmRes.toArray()) { (byIdx[r.index_code] ||= {})[r.dt] = r.nav; }
    const colors = { HS300: "#c14545", CSI800: "#6e9a4f", CSI500: "#c89c2b" };
    const cn = { HS300: "沪深300", CSI800: "中证800", CSI500: "中证500" };
    for (const idx of ["HS300", "CSI800", "CSI500"]) {
      const mm = byIdx[idx]; if (!mm) continue;
      const aligned = x.map(m => (m in mm ? mm[m] : null));
      const b = aligned.find(v => v !== null);
      series.push({ name: `${cn[idx]}(基准)`, type: "line", symbol: "none", connectNulls: true,
        data: b ? aligned.map(v => v === null ? null : v / b) : aligned,
        color: colors[idx],
        lineStyle: { width: 1.2, type: "dashed" } });
    }
  }
  cmpNavChart = echarts.init(div);
  cmpNavChart.setOption({
    grid: { left: 50, right: 20, top: 30, bottom: 30 },
    tooltip: { trigger: "axis" }, legend: { top: 0, textStyle: { fontSize: 11 }, itemWidth: 32 },
    xAxis: { type: "category", data: x, axisLabel: { fontSize: 10 } },
    yAxis: { type: "value", scale: true }, series,
  });
}

async function renderCmpIc() {
  const div = document.getElementById("cmp-ic-chart");
  if (cmpIcChart) { cmpIcChart.dispose(); cmpIcChart = null; }
  div.innerHTML = "";
  if (state.compareFactors.length === 0) { div.innerHTML = `<div class="empty">选因子后显示</div>`; return; }

  // IC 与持仓数无关 → 按因子去重
  const uniqCodes = [...new Set(state.compareFactors.map(f => f.code))];
  const inList = uniqCodes.map(c => `'${c}'`).join(",");
  // 12 月滚动 IC 均值，平滑噪声，更易对比
  const res = await state.db.query(`
    SELECT factor_code, strftime(month,'%Y-%m') AS dt,
           AVG(ic) OVER (PARTITION BY factor_code ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS ic12
    FROM factor_ic WHERE factor_code IN (${inList}) AND NOT ISNAN(ic)
    ORDER BY factor_code, month
  `);
  const byF = {};
  for (const r of res.toArray()) { (byF[r.factor_code] ||= { dt: [], ic: [] }); byF[r.factor_code].dt.push(r.dt); byF[r.factor_code].ic.push(r.ic12); }
  const x = (byF[uniqCodes[0]] || { dt: [] }).dt;
  const series = uniqCodes.map((code, i) => {
    const s = byF[code]; if (!s) return null;
    return { name: code, type: "line", symbol: "none", data: s.ic,
             color: STRAT_COLORS[i % STRAT_COLORS.length],
             lineStyle: { width: 1.6 } };
  }).filter(Boolean);

  cmpIcChart = echarts.init(div);
  cmpIcChart.setOption({
    grid: { left: 50, right: 20, top: 30, bottom: 30 },
    tooltip: { trigger: "axis" }, legend: { top: 0, textStyle: { fontSize: 11 } },
    xAxis: { type: "category", data: x, axisLabel: { fontSize: 10 } },
    yAxis: { type: "value", name: "12月滚动IC" },
    series,
    visualMap: undefined,
    markLine: undefined,
  });
}

async function renderCmpCorr() {
  const div = document.getElementById("cmp-corr-chart");
  if (cmpCorrChart) { cmpCorrChart.dispose(); cmpCorrChart = null; }
  div.innerHTML = "";
  if (!state.hasCorr) { div.innerHTML = `<div class="empty">相关性数据未生成（需跑 scripts/08_factor_corr.py）</div>`; return; }
  // 选中因子去重（同因子可重复加入不同 N，但相关性与 N 无关）；<2 个时显示全部
  const uniq = [...new Set(state.compareFactors.map(f => f.code))];
  const isAll = uniq.length < 2;
  // 全量模式：按一级/二级分类排序，让同类因子在热力图上聚成块，红/蓝色块一眼可辨
  let codes;
  if (isAll) {
    codes = [...state.catalog]
      .sort((a, b) => (a.l1 + a.l2).localeCompare(b.l1 + b.l2) || a.code.localeCompare(b.code))
      .map(f => f.code);
  } else {
    codes = uniq;
  }
  const inList = codes.map(c => `'${c}'`).join(",");
  const res = await state.db.query(`
    SELECT factor_a, factor_b, corr FROM factor_corr
    WHERE factor_a IN (${inList}) AND factor_b IN (${inList})
  `);
  const cmap = {};
  for (const r of res.toArray()) cmap[`${r.factor_a}|${r.factor_b}`] = r.corr;
  const data = [];
  codes.forEach((a, i) => codes.forEach((b, j) => {
    const c = cmap[`${a}|${b}`];
    data.push([j, i, c === null || c === undefined ? "-" : +c.toFixed(2)]);
  }));

  const n = codes.length;
  // 自适应尺寸：每格约 18px，让格子接近正方形、字够清。
  // 全量 46 → ~830px 见方，超出面板宽度时由外层容器横向滚动（见下方 overflow）。
  const cell = n > 20 ? 17 : 26;
  const plotH = n * cell + 110;          // 上下留刻度 + 图例
  const plotW = n * cell + 110;          // 左右留 y 轴标签
  div.style.height = plotH + "px";
  // 横向：全量模式下让图比面板宽，外层 panel 横向滚动，避免 46 列挤成糊
  div.style.width = (n > 16 ? plotW + "px" : "100%");
  div.style.minWidth = "0";
  div.parentElement.style.overflowX = (n > 16 ? "auto" : "visible");
  // 格子里的数字：因子多了必糊，>16 个时关掉，靠颜色 + 悬停 tooltip；少量因子才标数值
  const showLabel = n <= 16;
  const labelFont = n <= 10 ? 11 : 9;
  const axisFont = n > 30 ? 9 : (n > 16 ? 10 : 11);

  cmpCorrChart = echarts.init(div);
  cmpCorrChart.setOption({
    grid: { left: 90, right: 20, top: 16, bottom: 70 },
    tooltip: { position: "top", formatter: p => `${codes[p.data[1]]} × ${codes[p.data[0]]}<br/>corr: ${p.data[2]}` },
    xAxis: { type: "category", data: codes, axisLabel: { fontSize: axisFont, rotate: 90, interval: 0 } },
    yAxis: { type: "category", data: codes, axisLabel: { fontSize: axisFont, interval: 0 } },
    visualMap: { min: -1, max: 1, calculable: true, orient: "horizontal", left: "center", bottom: 0,
                 inRange: { color: ["#c14545", "#ffffff", "#1a4d80"] }, textStyle: { fontSize: 10 } },
    series: [{ type: "heatmap", data,
               label: { show: showLabel, fontSize: labelFont, formatter: p => p.data[2] },
               itemStyle: { borderColor: "#fff", borderWidth: n > 20 ? 0.5 : 1 },
               emphasis: { itemStyle: { shadowBlur: 6, borderColor: "#333", borderWidth: 1 } } }],
  });
}

function bindModeButtons() {
  document.querySelectorAll(".mode-btn").forEach(b => {
    b.onclick = () => switchMode(b.dataset.mode);
  });
}

function bindCmpDefaultButtons() {
  // 默认持仓数（仅影响之后新加入的因子）
  document.querySelectorAll(".cmpdef-btn[data-n]").forEach(b => {
    b.onclick = () => {
      state.compareDefaultN = parseInt(b.dataset.n, 10);
      document.querySelectorAll(".cmpdef-btn").forEach(x => x.classList.remove("active"));
      b.classList.add("active");
    };
  });
}

// ===================== 因子排行榜 =====================

// 排行榜列定义：key 用于排序，label 表头，fmt 格式化，good=+1 表示越大越好（综合分方向用）
const RANK_COLS = [
  { key: "rank",      label: "#",       lcol: true,  fmt: v => v },
  { key: "code",      label: "因子",    lcol: true,  fmt: v => v },
  { key: "name_cn",   label: "名称",    lcol: true,  fmt: v => v },
  { key: "score",     label: "综合分",  fmt: v => v.toFixed(2), cls: "score-cell" },
  { key: "annual",    label: "年化",    fmt: v => (v * 100).toFixed(1) + "%" },
  { key: "sharpe",    label: "夏普",    fmt: v => v.toFixed(2) },
  { key: "mdd",       label: "最大回撤", fmt: v => (v * 100).toFixed(1) + "%" },
  { key: "winRate",   label: "月胜率",  fmt: v => (v * 100).toFixed(0) + "%" },
  { key: "rankIC",    label: "RankIC均值", fmt: v => v.toFixed(3) },
  { key: "icir",      label: "IC_IR",   fmt: v => v.toFixed(2) },
];

let _rankState = { rows: null, sortKey: "score", desc: true, checked: new Set(),
                   range: "all", start: null, end: null };

let _rankBarBound = false;
async function renderRanking() {
  const box = document.getElementById("rank-table");
  try {
    await ensureDB();   // 先确保 DuckDB 就绪，区间下拉/排名查询都依赖它
    if (!_rankBarBound) {
      document.getElementById("rank-to-compare").onclick = () => rankSendTo("compare");
      document.getElementById("rank-to-compose").onclick = () => rankSendTo("compose");
      document.getElementById("rank-clear-sel").onclick = () => { _rankState.checked.clear(); drawRankTable(); };
      await initRankRangeControls();
      _rankBarBound = true;
    }
    if (!_rankState.rows) {
      box.innerHTML = `<div class="empty">计算中…</div>`;
      _rankState.rows = await computeRanking(_rankState.start, _rankState.end);
    }
    drawRankTable();
  } catch (err) {
    console.error("renderRanking failed:", err);
    box.innerHTML = `<pre style="color:#c00;white-space:pre-wrap;font-size:11px">排行榜失败：${err.message || err}</pre>`;
  }
}

// 所有可选月份（YYYY-MM），升序。用于自定义起止下拉 + 区间预设换算。
let _rankMonths = null;
async function rankMonths() {
  if (_rankMonths) return _rankMonths;
  const res = await state.db.query(
    `SELECT DISTINCT strftime(trade_date,'%Y-%m') m FROM preset_backtest ORDER BY m`);
  _rankMonths = res.toArray().map(r => r.m);
  return _rankMonths;
}

// 把预设区间换算成 [startMonth, endMonth]（含端点，YYYY-MM）
function rangeToBounds(range, months) {
  const last = months[months.length - 1];
  if (range === "all") return [months[0], last];
  if (range === "1y") return [months[Math.max(0, months.length - 12)], last];
  if (range === "3y") return [months[Math.max(0, months.length - 36)], last];
  if (/^\d{4}$/.test(range)) return [`${range}-01`, `${range}-12`];
  return [months[0], last];
}

async function initRankRangeControls() {
  const months = await rankMonths();
  // 填充自定义起止下拉
  const startSel = document.getElementById("rk-start");
  const endSel = document.getElementById("rk-end");
  startSel.innerHTML = months.map(m => `<option value="${m}">${m}</option>`).join("");
  endSel.innerHTML = months.map(m => `<option value="${m}">${m}</option>`).join("");
  startSel.value = months[0];
  endSel.value = months[months.length - 1];
  _rankState.start = months[0];
  _rankState.end = months[months.length - 1];
  // 预设区间按钮
  document.querySelectorAll(".rkrange-btn").forEach(b => {
    b.onclick = async () => {
      document.querySelectorAll(".rkrange-btn").forEach(x => x.classList.remove("active"));
      b.classList.add("active");
      _rankState.range = b.dataset.range;
      const [s, e] = rangeToBounds(b.dataset.range, months);
      _rankState.start = s; _rankState.end = e;
      startSel.value = s; endSel.value = e;
      await recomputeRank();
    };
  });
  // 自定义下拉
  const onCustom = async () => {
    document.querySelectorAll(".rkrange-btn").forEach(x => x.classList.remove("active"));
    _rankState.range = "custom";
    let s = startSel.value, e = endSel.value;
    if (s > e) { e = s; endSel.value = s; }   // 防起点晚于终点
    _rankState.start = s; _rankState.end = e;
    await recomputeRank();
  };
  startSel.onchange = onCustom;
  endSel.onchange = onCustom;
}

async function recomputeRank() {
  const box = document.getElementById("rank-table");
  box.innerHTML = `<div class="empty">按区间重新计算中…</div>`;
  _rankState.rows = await computeRanking(_rankState.start, _rankState.end);
  drawRankTable();
}

// startMonth/endMonth: 'YYYY-MM'（含端点）；null 表示不限。
async function computeRanking(startMonth, endMonth) {
  // 区间过滤条件（作用于 trade_date / month）
  const btWhere = ["top_n = 30"];
  const icWhere = [];
  if (startMonth) { btWhere.push(`strftime(trade_date,'%Y-%m') >= '${startMonth}'`); icWhere.push(`strftime(month,'%Y-%m') >= '${startMonth}'`); }
  if (endMonth)   { btWhere.push(`strftime(trade_date,'%Y-%m') <= '${endMonth}'`);   icWhere.push(`strftime(month,'%Y-%m') <= '${endMonth}'`); }
  const icWhereSql = icWhere.length ? "WHERE " + icWhere.join(" AND ") : "";

  // 1) top-30 区间内的月度收益 → 在区间内重建 NAV（从 1.0 起），再算年化/夏普/回撤/胜率
  const btRes = await state.db.query(`
    SELECT factor_code, port_ret FROM preset_backtest
    WHERE ${btWhere.join(" AND ")} ORDER BY factor_code, trade_date
  `);
  const series = new Map();   // code → {rets, navs}
  for (const r of btRes.toArray()) {
    if (!series.has(r.factor_code)) series.set(r.factor_code, { rets: [], navs: [] });
    const o = series.get(r.factor_code);
    o.rets.push(r.port_ret);
    const prev = o.navs.length ? o.navs[o.navs.length - 1] : 1;
    o.navs.push(prev * (1 + r.port_ret));   // 区间内重建净值，保证回撤/年化口径对齐区间
  }
  // 2) IC 统计：区间内 RankIC 均值 + IC_IR（= RankIC均值 / RankIC标准差 × √12，年化）
  const icRes = await state.db.query(`
    SELECT factor_code,
           AVG(rank_ic) AS rank_ic_mean,
           STDDEV_SAMP(rank_ic) AS rank_ic_std,
           COUNT(rank_ic) AS n
    FROM factor_ic ${icWhereSql} GROUP BY factor_code
  `);
  const icStat = new Map();
  for (const r of icRes.toArray()) {
    const ir = (r.rank_ic_std && r.rank_ic_std > 0) ? r.rank_ic_mean / r.rank_ic_std * Math.sqrt(12) : 0;
    icStat.set(r.factor_code, { rankIC: r.rank_ic_mean ?? 0, icir: ir });
  }
  // 3) 每因子汇总指标
  const rows = [];
  for (const f of state.catalog) {
    const s = series.get(f.code);
    const m = s ? computeMetrics(s.rets, s.navs) : null;
    const ic = icStat.get(f.code) || { rankIC: 0, icir: 0 };
    if (!m) continue;
    rows.push({
      code: f.code, name_cn: f.name_cn, l1: f.l1, l2: f.l2,
      annual: m.annual, sharpe: m.sharpe, mdd: m.mdd, winRate: m.winRate,
      rankIC: ic.rankIC, icir: ic.icir,
      nMonths: s.rets.length,
    });
  }
  // 4) 综合分：各分项在全因子截面 z-score 后加权。
  //    有效性(50%)：RankIC均值 25% + IC_IR 25%
  //    业绩(50%)：年化 15% + 夏普 15% + 最大回撤 10%(取负，回撤越小越好) + 月胜率 10%
  const zget = makeZScorer(rows);
  const W = { rankIC: .25, icir: .25, annual: .15, sharpe: .15, mdd: .10, winRate: .10 };
  for (const r of rows) {
    r.score =
      W.rankIC * zget("rankIC", r.rankIC) +
      W.icir   * zget("icir", r.icir) +
      W.annual * zget("annual", r.annual) +
      W.sharpe * zget("sharpe", r.sharpe) +
      W.mdd    * (-zget("mdd", r.mdd)) +     // 回撤是负数，越接近0越好 → z 越大越好，但方向上"大回撤"=更负，故取负使"小回撤"得高分
      W.winRate * zget("winRate", r.winRate);
  }
  return rows;
}

// 返回一个 (key, value) → z-score 的函数（基于 rows 中该 key 的均值/标准差）
function makeZScorer(rows) {
  const stats = {};
  return (key, val) => {
    if (!stats[key]) {
      const xs = rows.map(r => r[key]).filter(v => Number.isFinite(v));
      const mean = xs.reduce((s, v) => s + v, 0) / xs.length;
      const std = Math.sqrt(xs.reduce((s, v) => s + (v - mean) ** 2, 0) / xs.length) || 1;
      stats[key] = { mean, std };
    }
    return (val - stats[key].mean) / stats[key].std;
  };
}

function drawRankTable() {
  const box = document.getElementById("rank-table");
  const { sortKey, desc } = _rankState;
  // 区间提示 + 样本月数
  const info = document.getElementById("rk-range-info");
  if (info) {
    const nMonths = _rankState.rows[0]?.nMonths;
    info.textContent = `区间 ${_rankState.start} ~ ${_rankState.end}` + (nMonths ? `（${nMonths} 个月）` : "");
  }
  // mdd 排序特殊：值是负数，"越大(越接近0)越好"，默认降序即可；其它指标同理降序=好在前
  const sorted = [..._rankState.rows].sort((a, b) => {
    const av = a[sortKey], bv = b[sortKey];
    if (typeof av === "string") return desc ? bv.localeCompare(av) : av.localeCompare(bv);
    return desc ? bv - av : av - bv;
  });
  const ths = RANK_COLS.map(c =>
    `<th class="${c.lcol ? "lcol " : ""}${c.key === sortKey ? "sorted" : ""}" data-key="${c.key}">${c.label}${c.key === sortKey ? (desc ? " ▼" : " ▲") : ""}</th>`
  ).join("");
  // 首列：勾选框（含全选）
  const allChecked = sorted.length > 0 && sorted.every(r => _rankState.checked.has(r.code));
  let html = `<table class="rank-table"><thead><tr>` +
    `<th class="lcol" style="cursor:default"><input type="checkbox" id="rank-check-all" ${allChecked ? "checked" : ""}></th>` +
    `${ths}</tr></thead><tbody>`;
  sorted.forEach((r, i) => {
    r._rank = i + 1;
    const topCls = (sortKey === "score" && desc && i < 5) ? "top-rank" : "";
    const chk = `<td class="lcol"><input type="checkbox" class="rank-chk" data-code="${r.code}" ${_rankState.checked.has(r.code) ? "checked" : ""}></td>`;
    const tds = RANK_COLS.map(c => {
      const cls = (c.lcol ? "lcol " : "") + (c.cls || "");
      let val;
      if (c.key === "rank") val = r._rank;
      else val = c.fmt(r[c.key]);
      return `<td class="${cls.trim()}">${val}</td>`;
    }).join("");
    html += `<tr class="${topCls}">${chk}${tds}</tr>`;
  });
  html += `</tbody></table>`;
  box.innerHTML = html;
  // 列头点击排序（勾选列除外）
  box.querySelectorAll("th[data-key]").forEach(th => {
    th.onclick = () => {
      const k = th.dataset.key;
      if (k === "rank") return;
      if (_rankState.sortKey === k) _rankState.desc = !_rankState.desc;
      else { _rankState.sortKey = k; _rankState.desc = true; }
      drawRankTable();
    };
  });
  // 勾选框
  box.querySelectorAll(".rank-chk").forEach(cb => {
    cb.onchange = () => {
      if (cb.checked) _rankState.checked.add(cb.dataset.code);
      else _rankState.checked.delete(cb.dataset.code);
      updateRankSelCount();
    };
  });
  const all = document.getElementById("rank-check-all");
  if (all) all.onchange = () => {
    if (all.checked) sorted.forEach(r => _rankState.checked.add(r.code));
    else sorted.forEach(r => _rankState.checked.delete(r.code));
    drawRankTable();
  };
  updateRankSelCount();
}

function updateRankSelCount() {
  const el = document.getElementById("rank-sel-count");
  if (el) el.textContent = `已选 ${_rankState.checked.size} 个`;
}

// 把排行榜勾选的因子带入 对比 / 合成，并切到对应 tab
function rankSendTo(mode) {
  const codes = [..._rankState.checked];
  if (codes.length === 0) { alert("请先勾选至少一个因子"); return; }
  if (mode === "compare") {
    state.compareFactors = codes.map(code => ({ code, n: state.compareDefaultN }));
  } else {
    state.composeFactors = codes.map(code => ({ code, weight: 1, op: ">=", thr: null }));
  }
  switchMode(mode);
}

// ===================== 多因子合成 =====================

// 按"当前所选因子集"缓存一张窄表 cps_base，避免每次调权重/阈值都重扫 65MB parquet。
// cps_base 只含选中因子的 (trade_date, stock_code, factor_code, score)，几万~几十万行，
// 物化进内存后，stocks/backtest/最优权重 全部改查它，纯内存聚合，调权重近乎瞬时。
// 因子集变化（增删因子）才重建；权重、阈值、N 改变不触发重建。
let _cpsBaseKey = null;
let _cpsBaseBuild = null;     // 进行中的重建 promise（串行锁）
async function ensureComposeBase() {
  const codes = state.composeFactors.map(f => f.code).sort();
  const key = codes.join(",");
  // 若已有重建在跑，先等它结束（快速连点多个因子时，多次 renderCompose 并发调用本函数；
  // 不串行化会让 DROP/CREATE 交错 → "Table cps_base already exists"）。等完后用最新 key 复判。
  if (_cpsBaseBuild) { try { await _cpsBaseBuild; } catch (_) {} }
  if (key === _cpsBaseKey) return;          // 因子集未变，复用缓存
  _cpsBaseBuild = (async () => {
    // DROP→CREATE 用 CREATE OR REPLACE 保证幂等；先置 key 失效，建好再写回。
    _cpsBaseKey = null;
    if (codes.length === 0) {
      await state.db.query(`DROP TABLE IF EXISTS cps_base`);
    } else {
      const inList = codes.map(c => `'${c}'`).join(",");
      // 一次性从 parquet view 取选中因子（DuckDB 借 factor_code 聚簇做 row-group 裁剪 + Range 读），
      // 物化成小表。后续所有合成查询不再碰 parquet。
      await state.db.query(`
        CREATE OR REPLACE TABLE cps_base AS
        SELECT trade_date, stock_code, factor_code, score
        FROM factor_score_full
        WHERE factor_code IN (${inList}) AND score IS NOT NULL
      `);
    }
    _cpsBaseKey = key;
  })();
  try { await _cpsBaseBuild; } finally { _cpsBaseBuild = null; }
}

function toggleComposeFactor(code) {
  const i = state.composeFactors.findIndex(f => f.code === code);
  if (i >= 0) state.composeFactors.splice(i, 1);
  else state.composeFactors.push({ code, weight: 1, op: ">=", thr: null });
  updateTreeHighlight();
  renderCompose();
}

// 过滤条件 SQL 片段：返回 {cte, join, nConds}。基于设了阈值(thr非null)的因子。
function composeCond() {
  const conds = state.composeFactors.filter(f => f.thr !== null && Number.isFinite(f.thr));
  if (conds.length === 0) return { cte: "", join: "", nConds: 0 };
  const orC = conds.map(f => `(factor_code='${f.code}' AND score ${f.op} ${f.thr})`).join(" OR ");
  return {
    cte: `cond AS (SELECT trade_date, stock_code, COUNT(*) AS p FROM cps_base
            WHERE score IS NOT NULL AND (${orC}) GROUP BY trade_date, stock_code),`,
    join: `JOIN cond cd ON cd.trade_date = c.trade_date AND cd.stock_code = c.stock_code AND cd.p = ${conds.length}`,
    nConds: conds.length,
  };
}

function removeComposeAt(i) {
  state.composeFactors.splice(i, 1);
  updateTreeHighlight();
  renderCompose();
}

// 合成 SQL 的 VALUES 子句： (VALUES ('PE',0.4),('ROE',0.6)) w(code,weight)
function composeValues() {
  return state.composeFactors.map(f => `('${f.code}',${f.weight})`).join(",");
}

function renderComposeControls() {
  const box = document.getElementById("cps-controls");
  if (state.composeFactors.length === 0) { box.innerHTML = `<div class="empty">未选因子</div>`; return; }
  const wsum = state.composeFactors.reduce((s, f) => s + Math.abs(f.weight), 0) || 1;
  box.innerHTML = state.composeFactors.map((f, i) => {
    const pctw = (f.weight / wsum * 100).toFixed(0);
    return `<div class="cps-frow" style="display:flex;align-items:center;gap:6px;margin-bottom:6px;flex-wrap:wrap">
      <span style="width:10px;height:10px;border-radius:50%;background:${STRAT_COLORS[i % STRAT_COLORS.length]};display:inline-block"></span>
      <b style="font-size:12px;min-width:72px">${f.code}</b>
      <span style="color:#888;font-size:11px">权重</span>
      <input class="cps-w-input" data-idx="${i}" type="number" step="0.1" value="${f.weight}"
             style="width:50px;padding:2px 4px;border:1px solid #ccc;border-radius:3px;font-size:12px" />
      <span style="color:#888;font-size:11px">(${pctw}%)</span>
      <span style="color:#bbb">|</span>
      <span style="color:#888;font-size:11px">过滤 得分</span>
      <select class="cps-op" data-idx="${i}" style="font-size:12px;padding:2px;border:1px solid #ccc;border-radius:3px">
        <option value=">="${f.op === ">=" ? " selected" : ""}>≥</option>
        <option value="<="${f.op === "<=" ? " selected" : ""}>≤</option>
      </select>
      <input class="cps-thr" data-idx="${i}" type="number" step="0.5" placeholder="不限"
             value="${f.thr === null ? "" : f.thr}"
             style="width:54px;padding:2px 4px;border:1px solid #ccc;border-radius:3px;font-size:12px" />
      <span class="cps-remove" data-idx="${i}" style="cursor:pointer;color:#c14545;font-size:13px;padding:0 4px">×</span>
    </div>`;
  }).join("");
  box.querySelectorAll(".cps-w-input").forEach(inp => {
    inp.addEventListener("change", () => {
      const f = state.composeFactors[parseInt(inp.dataset.idx, 10)];
      if (!f) return;
      const w = parseFloat(inp.value);
      if (!Number.isFinite(w)) { inp.value = f.weight; return; }
      f.weight = w; renderCompose();
    });
  });
  box.querySelectorAll(".cps-op").forEach(sel => sel.onchange = () => {
    state.composeFactors[parseInt(sel.dataset.idx, 10)].op = sel.value;
    if (state.composeFactors[parseInt(sel.dataset.idx, 10)].thr !== null) renderCompose();
  });
  box.querySelectorAll(".cps-thr").forEach(inp => {
    inp.addEventListener("change", () => {
      const f = state.composeFactors[parseInt(inp.dataset.idx, 10)];
      if (!f) return;
      const v = inp.value.trim();
      f.thr = v === "" ? null : (Number.isFinite(parseFloat(v)) ? parseFloat(v) : null);
      renderCompose();
    });
  });
  box.querySelectorAll(".cps-remove").forEach(x => {
    x.onclick = () => removeComposeAt(parseInt(x.dataset.idx, 10));
  });
}

let _composeLoadedOnce = false;
async function renderCompose() {
  document.getElementById("cps-selected").textContent =
    state.composeFactors.length ? `（已选 ${state.composeFactors.length} 个因子）` : "";
  renderComposeControls();
  // 首次进入合成需下载约 38MB 因子全历史数据，给明确提示（避免误以为卡死）
  if (!_composeLoadedOnce && state.composeFactors.length > 0) {
    document.getElementById("cps-stocks").innerHTML =
      `<h3>合成 Top 股票</h3><div class="empty">首次加载合成数据（约 38MB），请稍候…</div>`;
  }
  try {
    await ensureDB();
    await ensureComposeData();   // 懒加载合成专用大表
    _composeLoadedOnce = true;
    if (!state.hasComposeData) {
      document.getElementById("cps-stocks").innerHTML =
        `<h3>合成 Top 股票</h3><div class="empty">合成数据未生成（需跑 scripts/09_export_compose_data.py）</div>`;
      return;
    }
    if (state.composeFactors.length === 0) {
      await ensureComposeBase();   // 清掉缓存窄表
      document.getElementById("cps-stocks").innerHTML = `<h3>合成 Top 股票</h3><div class="empty">选因子后显示</div>`;
      document.getElementById("cps-kpi").innerHTML = `<div class="empty">选因子后显示</div>`;
      if (cpsNavChart) { cpsNavChart.dispose(); cpsNavChart = null; }
      document.getElementById("cps-nav-chart").innerHTML = "";
      return;
    }
    await ensureComposeBase();   // 因子集变了才重建窄表；权重/阈值/N 变则复用缓存
    await Promise.all([renderComposeStocks(), renderComposeBacktest()]);
  } catch (err) {
    console.error("renderCompose failed:", err);
    document.getElementById("cps-stocks").innerHTML =
      `<pre style="color:#c00;white-space:pre-wrap;font-size:11px">合成失败：${err.message || err}\n\n${err.stack || ""}</pre>`;
  }
}

async function renderComposeStocks() {
  const target = document.getElementById("cps-stocks");
  const nF = state.composeFactors.length;
  const cond = composeCond();
  const res = await state.db.query(`
    WITH w(code, weight) AS (VALUES ${composeValues()}),
    ${cond.cte}
    comp AS (
      SELECT s.trade_date, s.stock_code, SUM(s.score * w.weight) AS cs, COUNT(*) AS cnt
      FROM cps_base s JOIN w ON s.factor_code = w.code
      WHERE s.score IS NOT NULL
      GROUP BY s.trade_date, s.stock_code
    )
    SELECT c.stock_code, m.name, c.cs AS comp_score, CAST(c.trade_date AS VARCHAR) AS dt,
           d.industry_sw1, d.industry_sw2, d.market_cap, d.pe, d.pb, d.avg_amount
    FROM comp c
    ${cond.join}
    LEFT JOIN stock_meta m ON m.stock_code = c.stock_code
    LEFT JOIN stock_descriptors d ON d.stock_code = c.stock_code
    WHERE c.trade_date = (SELECT MAX(trade_date) FROM comp) AND c.cnt = ${nF}
      AND COALESCE(m.is_st, FALSE) = FALSE
      AND COALESCE(m.is_active_latest, FALSE) = TRUE
    ORDER BY c.cs DESC
    LIMIT ${state.composeN}
  `);
  const rows = res.toArray();
  const condDesc = state.composeFactors.filter(f => f.thr !== null && Number.isFinite(f.thr))
    .map(f => `${f.code}得分${f.op}${f.thr}`).join(" 且 ");
  if (rows.length === 0) {
    target.innerHTML = `<h3>合成 Top 股票</h3><div class="empty">无股票满足条件${condDesc ? "：" + condDesc : ""}（过滤可能过严，放宽阈值）</div>`;
    return;
  }
  const dt = rows[0].dt;
  const wdesc = state.composeFactors.map(f => `${f.code}×${f.weight}`).join(" + ");
  const fmt = (v, dp = 2) => (v === null || v === undefined ? "—" : Number(v).toFixed(dp));
  const fmtMV = (v) => (v === null || v === undefined ? "—" : (Number(v) / 1e4).toFixed(0));
  let html = `<h3>合成 Top ${state.composeN} 股票（截面日 ${dt}）</h3>
    <p style="color:#888;font-size:11px;margin:-4px 0 8px 0">合成得分 = ${wdesc}（z-score 加权和）${condDesc ? "；过滤：" + condDesc : ""}（已剔 ST/停牌）</p>
    <table class="stock-table"><thead><tr>
      <th>#</th><th>代码</th><th>名称</th><th>申万一级</th><th>市值(亿)</th><th>PE</th><th>PB</th><th>合成得分</th>
    </tr></thead><tbody>`;
  rows.forEach((r, i) => {
    html += `<tr><td>${i + 1}</td><td>${r.stock_code}</td><td>${r.name || ""}</td>
      <td>${r.industry_sw1 || "—"}</td><td>${fmtMV(r.market_cap)}</td>
      <td>${fmt(r.pe, 1)}</td><td>${fmt(r.pb, 2)}</td><td>${fmt(r.comp_score, 3)}</td></tr>`;
  });
  target.innerHTML = html + "</tbody></table>";
}

async function renderComposeBacktest() {
  const nF = state.composeFactors.length;
  document.getElementById("cps-nav-title").textContent =
    `合成组合净值（top-${state.composeN}，月末等权调仓，0.2% 双边成本，起点=1.0）`;
  const cond = composeCond();
  // 逐月：先按条件过滤（cond），再按合成得分排序选 top-N（剔 ST、当月可交易），取下月收益
  const res = await state.db.query(`
    WITH w(code, weight) AS (VALUES ${composeValues()}),
    ${cond.cte}
    comp AS (
      SELECT s.trade_date, s.stock_code, SUM(s.score * w.weight) AS cs, COUNT(*) AS cnt
      FROM cps_base s JOIN w ON s.factor_code = w.code
      WHERE s.score IS NOT NULL
      GROUP BY s.trade_date, s.stock_code
    ),
    ranked AS (
      SELECT c.trade_date, c.stock_code, r.fwd_return,
             ROW_NUMBER() OVER (PARTITION BY c.trade_date ORDER BY c.cs DESC) AS rk
      FROM comp c
      ${cond.join}
      JOIN stock_meta m ON m.stock_code = c.stock_code
      JOIN monthly_return r ON r.trade_date = c.trade_date AND r.stock_code = c.stock_code
      WHERE c.cnt = ${nF} AND COALESCE(m.is_st, FALSE) = FALSE
    )
    SELECT strftime(trade_date, '%Y-%m') AS dt, stock_code, fwd_return
    FROM ranked WHERE rk <= ${state.composeN}
    ORDER BY trade_date
  `);
  // JS 按月聚合：gross 收益 + 换手 + 净收益 + 净值
  const byMonth = new Map();
  for (const r of res.toArray()) {
    if (!byMonth.has(r.dt)) byMonth.set(r.dt, { rets: [], stocks: new Set() });
    const o = byMonth.get(r.dt);
    o.stocks.add(r.stock_code);
    if (r.fwd_return !== null && r.fwd_return !== undefined) o.rets.push(r.fwd_return);
  }
  const months = [...byMonth.keys()].sort();
  const COST = 0.002;
  let prev = null, nav = 1;
  const x = [], navArr = [], retArr = [];
  for (const mth of months) {
    const o = byMonth.get(mth);
    const gross = o.rets.length ? o.rets.reduce((s, v) => s + v, 0) / o.rets.length : 0;
    let turnover = 1;
    if (prev) {
      let diff = 0;
      for (const s of o.stocks) if (!prev.has(s)) diff++;
      for (const s of prev) if (!o.stocks.has(s)) diff++;
      turnover = diff / (2 * state.composeN);
    }
    const net = gross - 2 * COST * turnover;
    nav *= (1 + net);
    x.push(mth); navArr.push(nav); retArr.push(net);
    prev = o.stocks;
  }

  // 画净值 + 基准
  const div = document.getElementById("cps-nav-chart");
  if (cpsNavChart) { cpsNavChart.dispose(); cpsNavChart = null; }
  div.innerHTML = "";
  const series = [{ name: "合成组合", type: "line", symbol: "none", data: navArr,
                    color: "#1a4d80", lineStyle: { width: 2 } }];
  if (state.hasBenchmarks && x.length) {
    const bmRes = await state.db.query(`
      SELECT index_code, strftime(trade_date,'%Y-%m') AS dt, nav FROM benchmarks
      WHERE strftime(trade_date,'%Y-%m') >= '${x[0]}' AND strftime(trade_date,'%Y-%m') <= '${x[x.length-1]}'
      ORDER BY index_code, trade_date`);
    const byIdx = {};
    for (const r of bmRes.toArray()) (byIdx[r.index_code] ||= {})[r.dt] = r.nav;
    const colors = { HS300: "#c14545", CSI800: "#6e9a4f", CSI500: "#c89c2b" };
    const cn = { HS300: "沪深300", CSI800: "中证800", CSI500: "中证500" };
    for (const idx of ["HS300", "CSI800", "CSI500"]) {
      const mm = byIdx[idx]; if (!mm) continue;
      const aligned = x.map(m => (m in mm ? mm[m] : null));
      const b = aligned.find(v => v !== null);
      series.push({ name: `${cn[idx]}(基准)`, type: "line", symbol: "none", connectNulls: true,
        data: b ? aligned.map(v => v === null ? null : v / b) : aligned,
        color: colors[idx], lineStyle: { width: 1.2, type: "dashed" } });
    }
  }
  cpsNavChart = echarts.init(div);
  cpsNavChart.setOption({
    grid: { left: 50, right: 20, top: 30, bottom: 30 },
    tooltip: { trigger: "axis" }, legend: { top: 0, textStyle: { fontSize: 11 }, itemWidth: 32 },
    xAxis: { type: "category", data: x, axisLabel: { fontSize: 10 } },
    yAxis: { type: "value", scale: true }, series,
  });

  // KPI（合成组合行 + 三基准行）
  const m = computeMetrics(retArr, navArr);
  const ba = await benchAnnuals();
  const kdiv = document.getElementById("cps-kpi");
  if (!m) { kdiv.innerHTML = `<div class="empty">数据不足</div>`; return; }
  const pct = v => (v * 100).toFixed(1) + "%";
  const signed = v => (v >= 0 ? "+" : "") + (v * 100).toFixed(1) + "%";
  const ex300 = ("HS300" in ba) ? signed(m.annual - ba.HS300) : "—";
  const ex800 = ("CSI800" in ba) ? signed(m.annual - ba.CSI800) : "—";
  let krows = `<tr><td><b>合成组合</b></td><td>${pct(m.annual)}</td><td>${m.sharpe.toFixed(2)}</td><td>${pct(m.mdd)}</td>
      <td>${(m.winRate*100).toFixed(0)}%</td><td>${ex300}</td><td>${ex800}</td></tr>`;
  // 三基准行（绝对指标）
  if (state.hasBenchmarks) {
    const cn = { HS300: "沪深300", CSI800: "中证800", CSI500: "中证500" };
    const bRes = await state.db.query(`
      SELECT index_code, nav FROM benchmarks
      WHERE index_code IN ('HS300','CSI800','CSI500')
        AND trade_date BETWEEN (SELECT MIN(trade_date) FROM preset_backtest)
                           AND (SELECT MAX(trade_date) FROM preset_backtest)
      ORDER BY index_code, trade_date`);
    const bg = {};
    for (const r of bRes.toArray()) (bg[r.index_code] ||= []).push(r.nav);
    for (const idx of ["HS300", "CSI800", "CSI500"]) {
      const navs = bg[idx]; if (!navs || navs.length < 2) continue;
      const rets = navs.slice(1).map((v, i) => v / navs[i] - 1);
      const bm = computeMetrics(rets, navs);
      krows += `<tr style="color:#888;border-top:2px solid #ddd">
        <td style="color:#888">${cn[idx]}</td><td>${pct(bm.annual)}</td><td>${bm.sharpe.toFixed(2)}</td>
        <td>${pct(bm.mdd)}</td><td>${(bm.winRate*100).toFixed(0)}%</td><td>—</td><td>—</td></tr>`;
    }
  }
  kdiv.innerHTML = `<table class="kpi-table">
    <thead><tr><th>组合 / 基准</th><th>年化收益</th><th>夏普</th><th>最大回撤</th><th>月度胜率</th><th>超额vs300</th><th>超额vs800</th></tr></thead>
    <tbody>${krows}</tbody></table>`;
}

// ============ 最优权重网格搜索 ============

// 生成非负、和为 1、步长 step 的权重组合（nF 个因子）。用整数划分避免浮点误差。
function weightGrid(nF, step) {
  const steps = Math.round(1 / step);
  const res = [];
  function rec(idx, rem, acc) {
    if (idx === nF - 1) { res.push([...acc, rem / steps]); return; }
    for (let k = 0; k <= rem; k++) rec(idx + 1, rem - k, [...acc, k / steps]);
  }
  rec(0, steps, []);
  return res;
}

// 在 JS 内存里对一组权重跑合成回测，返回指标。conds=[{idx,op,thr}] 先过滤再打分。
function backtestWeights(monthsArr, weights, N, conds) {
  const COST = 0.002;
  let prev = null, nav = 1;
  const navArr = [], retArr = [];
  for (const mo of monthsArr) {
    let elig = mo.stocks;
    if (conds && conds.length) {
      elig = mo.stocks.filter(s => conds.every(c =>
        c.op === ">=" ? s.scores[c.idx] >= c.thr : s.scores[c.idx] <= c.thr));
    }
    if (elig.length === 0) {   // 该月无符合 → 空仓
      nav *= 1; navArr.push(nav); retArr.push(0); prev = new Set(); continue;
    }
    const scored = elig.map(s => {
      let c = 0; for (let i = 0; i < weights.length; i++) c += weights[i] * s.scores[i];
      return { code: s.code, comp: c, ret: s.ret };
    });
    scored.sort((a, b) => b.comp - a.comp);
    const picks = scored.slice(0, N);
    const gross = picks.reduce((s, p) => s + p.ret, 0) / picks.length;
    const cur = new Set(picks.map(p => p.code));
    let turnover = 1;
    if (prev) {
      let diff = 0;
      for (const c of cur) if (!prev.has(c)) diff++;
      for (const c of prev) if (!cur.has(c)) diff++;
      turnover = diff / (cur.size + prev.size || 1);
    }
    const net = gross - 2 * COST * turnover;
    nav *= (1 + net); navArr.push(nav); retArr.push(net); prev = cur;
  }
  return computeMetrics(retArr, navArr);
}

async function optimizeWeights() {
  const box = document.getElementById("cps-opt");
  const codes = state.composeFactors.map(f => f.code);
  const nF = codes.length;
  if (nF < 2) { box.innerHTML = `<div class="empty" style="color:#c14545">请先选 2 个以上因子</div>`; return; }
  if (nF > 4) { box.innerHTML = `<div class="empty" style="color:#c14545">最优权重仅支持 ≤4 个因子（组合爆炸）</div>`; return; }
  box.innerHTML = `<div class="loading">搜索中…</div>`;
  await ensureComposeData();
  await ensureComposeBase();

  // 一次拉取：所选因子得分 + 下月收益（已剔 ST、要求当月可交易）
  const inList = codes.map(c => `'${c}'`).join(",");
  // 候选股裁剪：只保留"在任一所选因子排进前 500"的股。合成 top-N(N≤100) 的成分
  // 必在此并集内（全因子都排 500 外 → 加权和必偏低 → 进不了 top），裁剪不改结果但大幅提速。
  const res = await state.db.query(`
    WITH base AS (
      SELECT s.trade_date, s.stock_code, s.factor_code, s.score, r.fwd_return
      FROM cps_base s
      JOIN stock_meta m USING(stock_code)
      JOIN monthly_return r ON r.trade_date = s.trade_date AND r.stock_code = s.stock_code
      WHERE s.factor_code IN (${inList}) AND s.score IS NOT NULL AND COALESCE(m.is_st, FALSE) = FALSE
    ),
    cand AS (
      SELECT DISTINCT trade_date, stock_code FROM (
        SELECT trade_date, stock_code,
               ROW_NUMBER() OVER (PARTITION BY trade_date, factor_code ORDER BY score DESC) AS rk
        FROM base
      ) WHERE rk <= 500
    )
    SELECT strftime(b.trade_date,'%Y-%m') AS ym, b.stock_code, b.factor_code, b.score, b.fwd_return
    FROM base b JOIN cand c ON c.trade_date = b.trade_date AND c.stock_code = b.stock_code
    ORDER BY b.trade_date
  `);
  // 组织成 months[ym] = { stocks: [{code, scores:[按codes顺序], ret}] }，仅保留所有因子都有得分的股
  const idxOf = Object.fromEntries(codes.map((c, i) => [c, i]));
  const tmp = new Map();   // ym -> Map(code -> {scores:[], ret, cnt})
  for (const r of res.toArray()) {
    if (!tmp.has(r.ym)) tmp.set(r.ym, new Map());
    const mm = tmp.get(r.ym);
    if (!mm.has(r.stock_code)) mm.set(r.stock_code, { scores: new Array(nF).fill(null), ret: r.fwd_return, cnt: 0 });
    const o = mm.get(r.stock_code);
    o.scores[idxOf[r.factor_code]] = r.score; o.cnt++;
  }
  const monthsArr = [];
  for (const [ym, mm] of tmp) {
    const stocks = [];
    for (const [code, o] of mm) if (o.cnt === nF) stocks.push({ code, scores: o.scores, ret: o.ret });
    if (stocks.length >= state.composeN) monthsArr.push({ ym, stocks });
  }
  monthsArr.sort((a, b) => a.ym < b.ym ? -1 : 1);

  // 过滤条件（JS 端）：因子在 codes 中的位置 idx + op + 阈值
  const conds = state.composeFactors
    .map((f, i) => (f.thr !== null && Number.isFinite(f.thr)) ? { idx: i, op: f.op, thr: f.thr } : null)
    .filter(Boolean);

  // 网格步长：因子越多步长越粗（控制组合数）
  const step = nF === 2 ? 0.05 : nF === 3 ? 0.1 : 0.2;
  const grid = weightGrid(nF, step);
  // 4 个目标各记录最优
  const best = {
    annual: { val: -Infinity, w: null, m: null },
    sharpe: { val: -Infinity, w: null, m: null },
    vol:    { val: Infinity,  w: null, m: null },
    mdd:    { val: -Infinity, w: null, m: null },   // mdd 是负数，越大(接近0)越好
  };
  for (const w of grid) {
    const m = backtestWeights(monthsArr, w, state.composeN, conds);
    if (!m) continue;
    if (m.annual > best.annual.val) best.annual = { val: m.annual, w, m };
    if (m.sharpe > best.sharpe.val) best.sharpe = { val: m.sharpe, w, m };
    if (m.vol < best.vol.val) best.vol = { val: m.vol, w, m };
    if (m.mdd > best.mdd.val) best.mdd = { val: m.mdd, w, m };
  }

  const pct = v => (v * 100).toFixed(1) + "%";
  const wstr = w => codes.map((c, i) => `${c} ${(w[i] * 100).toFixed(0)}%`).join(" / ");
  const targets = [
    ["年化收益最高", best.annual], ["夏普比率最高", best.sharpe],
    ["波动率最低", best.vol], ["最大回撤最小", best.mdd],
  ];
  let rows = "";
  targets.forEach(([label, b], ti) => {
    if (!b.w) return;
    rows += `<tr>
      <td>${label}</td>
      <td>${wstr(b.w)}</td>
      <td>${pct(b.m.annual)}</td><td>${b.m.sharpe.toFixed(2)}</td>
      <td>${pct(b.m.vol)}</td><td>${pct(b.m.mdd)}</td>
      <td><button class="cpsn-btn cps-apply" data-ti="${ti}">应用</button></td>
    </tr>`;
  });
  box.innerHTML = `
    <table class="opt-table">
      <thead><tr><th>优化目标</th><th>最优权重</th><th>年化</th><th>夏普</th><th>波动</th><th>回撤</th><th>操作</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
    <p style="color:#888;font-size:11px;margin-top:4px">网格步长 ${step}（${grid.length} 组组合），目标基于 top-${state.composeN} 历史回测。点"应用"把权重填回。</p>
    <p style="color:#c08040;font-size:11px;margin-top:2px">⚠ 这是<b>样本内</b>最优（2020-2025 回测期内最好的权重），不保证未来同样最优——实务中需警惕过拟合，建议结合因子逻辑而非只追历史最优。</p>`;
  // 应用按钮：把最优权重填回 composeFactors
  box.querySelectorAll(".cps-apply").forEach(btn => {
    btn.onclick = () => {
      const b = targets[parseInt(btn.dataset.ti, 10)][1];
      if (!b.w) return;
      b.w.forEach((wv, i) => { state.composeFactors[i].weight = +(wv).toFixed(3); });
      renderCompose();
    };
  });
}


function bindComposeButtons() {
  const optBtn = document.getElementById("cps-optimize");
  if (optBtn) optBtn.onclick = () => optimizeWeights().catch(e => {
    document.getElementById("cps-opt").innerHTML = `<pre style="color:#c00;font-size:11px">最优权重失败：${e.message}</pre>`;
  });
  document.querySelectorAll(".cpsn-btn[data-n]").forEach(b => {
    b.onclick = () => {
      state.composeN = parseInt(b.dataset.n, 10);
      document.querySelectorAll(".cpsn-btn").forEach(x => x.classList.remove("active"));
      b.classList.add("active");
      renderCompose();
    };
  });
  const inp = document.getElementById("cpsn-input");
  document.getElementById("cpsn-add").onclick = () => {
    const n = parseInt(inp.value, 10);
    if (!Number.isFinite(n) || n < 1 || n > 100) { inp.value = ""; return; }
    state.composeN = n;
    document.querySelectorAll(".cpsn-btn").forEach(x => x.classList.remove("active"));
    renderCompose();
  };
  inp.addEventListener("keydown", e => { if (e.key === "Enter") document.getElementById("cpsn-add").onclick(); });
}

function bindScanButtons() {
  document.querySelectorAll(".scan-btn").forEach(btn => {
    btn.onclick = () => {
      document.querySelectorAll(".scan-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      state.scanMetric = btn.dataset.metric;
      if (state.activeFactor) renderNScan(state.activeFactor);
    };
  });
}

bindScanButtons();
bindModeButtons();
bindCmpDefaultButtons();
bindComposeButtons();
init();
