// ── State ──
let allLakes = [];
let speciesNames = {};
let surveyCache = {};   // county -> survey data (lazy loaded)
let currentCounty = "";
let currentSpecies = "";
let currentSort = "cpue";
let currentDateYears = 0;  // 0 = any date
let currentLake = null;

// ── Boot ──
async function init() {
  const [lakes, counties, speciesList, species] = await Promise.all([
    fetchJSON("data/lakes.json"),
    fetchJSON("data/counties.json"),
    fetchJSON("data/species_list.json"),
    fetchJSON("data/species_names.json"),
  ]);

  allLakes = lakes;
  speciesNames = species;

  populateCountyDropdown(counties);
  populateSpeciesDropdown(speciesList);
  renderLakeList(allLakes, null);

  document.getElementById("county-select").addEventListener("change", onFilterChange);
  document.getElementById("species-select").addEventListener("change", onFilterChange);
  document.getElementById("sort-select").addEventListener("change", onSortChange);
  document.getElementById("date-select").addEventListener("change", onSortChange);
  document.getElementById("lake-search").addEventListener("input", onFilterChange);
}

// ── Data fetching ──
async function fetchJSON(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`Failed to load ${path}`);
  return res.json();
}

async function loadCountySurveys(county) {
  if (surveyCache[county]) return surveyCache[county];
  const safe = county.replace(/ /g, "_").replace(/\//g, "_");
  try {
    const data = await fetchJSON(`data/surveys/${safe}.json`);
    surveyCache[county] = data;
    return data;
  } catch {
    surveyCache[county] = {};
    return {};
  }
}

// Load all counties needed for "All Minnesota" ranking, with progress callback
async function loadAllCountySurveys(counties, onProgress) {
  let done = 0;
  await Promise.all(
    counties.map(async (county) => {
      await loadCountySurveys(county);
      done++;
      onProgress(done, counties.length);
    })
  );
}

// ── UI: Dropdowns ──
function populateCountyDropdown(counties) {
  const sel = document.getElementById("county-select");
  counties.forEach((c) => {
    const opt = document.createElement("option");
    opt.value = c;
    opt.textContent = c;
    sel.appendChild(opt);
  });
}

function populateSpeciesDropdown(speciesList) {
  const sel = document.getElementById("species-select");
  speciesList.forEach(({ code, name }) => {
    const opt = document.createElement("option");
    opt.value = code;
    opt.textContent = name;
    sel.appendChild(opt);
  });
}

// ── Event handlers ──
async function onFilterChange() {
  currentCounty = document.getElementById("county-select").value;
  currentSpecies = document.getElementById("species-select").value;

  const sortGroup = document.getElementById("sort-group");
  const dateGroup = document.getElementById("date-group");
  sortGroup.style.display = currentSpecies ? "" : "none";
  dateGroup.style.display = currentSpecies ? "" : "none";

  currentLake = null;
  showDetailPlaceholder();

  const lakes = filteredLakes();

  if (!currentSpecies) {
    renderLakeList(lakes, null);
    return;
  }

  // Need survey data to rank — figure out which counties to load
  const neededCounties = currentCounty
    ? [currentCounty]
    : [...new Set(lakes.map((l) => l.county))];

  const alreadyCached = neededCounties.every((c) => surveyCache[c]);
  if (!alreadyCached) {
    await loadAllCountySurveys(neededCounties, (done, total) => {
      document.getElementById("results-count").textContent =
        `Loading… ${done}/${total} counties`;
    });
  }

  const ranked = rankLakes(lakes, currentSpecies, currentSort, currentDateYears);
  renderLakeList(ranked.lakes, ranked.metricByDow);
}

function onSortChange() {
  currentSort = document.getElementById("sort-select").value;
  currentDateYears = parseInt(document.getElementById("date-select").value, 10);
  if (!currentSpecies) return;
  const lakes = filteredLakes();
  const ranked = rankLakes(lakes, currentSpecies, currentSort, currentDateYears);
  renderLakeList(ranked.lakes, ranked.metricByDow);
}

// ── Filtering & ranking ──
function filteredLakes() {
  const search = document.getElementById("lake-search").value.trim().toLowerCase();
  return allLakes.filter((l) => {
    const countyMatch = !currentCounty || l.county === currentCounty;
    const nameMatch = !search || l.name.toLowerCase().includes(search);
    return countyMatch && nameMatch;
  });
}

// For each lake, find the most recent Standard gill nets survey row for the
// chosen species, then sort descending by the chosen metric.
// dateYears: if > 0, only consider surveys from within the last N years.
function rankLakes(lakes, speciesCode, sortField, dateYears = 0) {
  const cutoff = dateYears > 0
    ? new Date(new Date().getFullYear() - dateYears, 0, 1).toISOString().slice(0, 10)
    : null;

  const metricByDow = {};

  for (const lake of lakes) {
    const countyData = surveyCache[lake.county] || {};
    let rows = (countyData[lake.id] || []).filter(
      (r) => r.species === speciesCode && r.gear === "Standard gill nets"
    );
    if (cutoff) rows = rows.filter((r) => r.date >= cutoff);
    if (rows.length === 0) continue;

    // Sort by date descending
    const sorted = [...rows].sort((a, b) => b.date.localeCompare(a.date));

    // Deduplicate by survey_id to get distinct surveys (a survey can have multiple rows per gear)
    const seen = new Set();
    const surveys = sorted.filter((r) => {
      if (seen.has(r.survey_id)) return false;
      seen.add(r.survey_id);
      return true;
    });

    const latest = surveys[0];
    const val = latest[sortField];
    if (val == null) continue;

    // Trend: compare latest value to previous survey's value for the same field
    let trend = null;
    if (surveys.length >= 2) {
      const prevVal = surveys[1][sortField];
      const currVal = latest[sortField];
      if (prevVal != null && currVal != null && prevVal !== 0) {
        trend = {
          direction: currVal > prevVal ? "up" : currVal < prevVal ? "down" : "same",
          prevVal,
          prevDate: surveys[1].date,
        };
      }
    }

    metricByDow[lake.id] = { value: val, date: latest.date, trend };
  }

  const ranked = lakes
    .filter((l) => metricByDow[l.id] != null)
    .sort((a, b) => metricByDow[b.id].value - metricByDow[a.id].value);

  return { lakes: ranked, metricByDow };
}

// ── UI: Lake list ──
function renderLakeList(lakes, metricByDow) {
  const ul = document.getElementById("lake-list");
  const noResults = document.getElementById("no-results");
  const count = document.getElementById("results-count");

  ul.innerHTML = "";

  const total = lakes.length;
  const label = metricByDow
    ? `${total.toLocaleString()} lake${total !== 1 ? "s" : ""} with data`
    : `${total.toLocaleString()} lake${total !== 1 ? "s" : ""}`;

  if (total === 0) {
    noResults.classList.remove("hidden");
    count.textContent = "";
    return;
  }

  noResults.classList.add("hidden");
  count.textContent = label;

  const sortLabel = {
    cpue: "CPUE",
    total_catch: "Catch",
    avg_length: "Avg size",
    avg_weight: "Avg weight",
  }[currentSort] || "";

  const frag = document.createDocumentFragment();
  lakes.forEach((lake, i) => {
    const li = document.createElement("li");
    li.dataset.id = lake.id;

    const metric = metricByDow ? metricByDow[lake.id] : null;
    const rankBadge = metricByDow
      ? `<span class="rank-badge">#${i + 1}</span>`
      : "";
    const metricBadge = metric
      ? `<span class="metric-badge">${sortLabel}: ${formatMetric(metric.value, currentSort)}</span>`
      : "";
    const dateBadge = metric
      ? `<span class="survey-date">Survey: ${metric.date}</span>`
      : "";
    const trendBadge = metric?.trend
      ? trendBadgeHtml(metric.trend, currentSort)
      : "";

    li.innerHTML = `
      <div class="lake-row-top">
        ${rankBadge}
        <span class="lake-name">${escHtml(lake.name)}</span>
        ${metricBadge}
      </div>
      <div class="lake-row-bottom">
        <span class="lake-county">${escHtml(lake.county)} County</span>
        ${dateBadge}
      </div>
      ${trendBadge ? `<div class="lake-row-trend">${trendBadge}</div>` : ""}
    `;
    li.addEventListener("click", () => selectLake(lake, li));
    frag.appendChild(li);
  });
  ul.appendChild(frag);
}

function formatMetric(val, field) {
  if (val == null) return "—";
  if (field === "total_catch") return val.toLocaleString();
  if (field === "avg_length") return `${val.toFixed(1)}"`;
  if (field === "avg_weight") return `${val.toFixed(2)} lbs`;
  return val.toFixed(2);
}

function trendBadgeHtml(trend, sortField) {
  const arrow = trend.direction === "up" ? "↑" : trend.direction === "down" ? "↓" : "→";
  const cls = trend.direction === "up" ? "trend-up" : trend.direction === "down" ? "trend-down" : "trend-same";
  const year = trend.prevDate.slice(0, 4);
  const formatted = formatMetric(trend.prevVal, sortField);
  return `<span class="trend-badge ${cls}">${arrow} from ${formatted} (${year})</span>`;
}

// ── Lake selection ──
async function selectLake(lake, li) {
  document.querySelectorAll("#lake-list li").forEach((el) => el.classList.remove("active"));
  li.classList.add("active");
  currentLake = lake;

  showDetailLoading(lake);
  const surveys = await loadCountySurveys(lake.county);
  renderDetail(lake, surveys[lake.id] || []);
}

// ── Detail panel ──
function showDetailPlaceholder() {
  document.getElementById("detail-panel").innerHTML = `
    <div class="detail-placeholder"><p>Select a lake to see its fish survey data.</p></div>
  `;
}

function showDetailLoading(lake) {
  document.getElementById("detail-panel").innerHTML = `
    <div class="detail-header">
      <div>
        <h2>${escHtml(lake.name)}</h2>
        <div class="meta">${escHtml(lake.county)} County &bull; DOW #${lake.id}</div>
      </div>
    </div>
    <p class="loading">Loading survey data…</p>
  `;
}

function renderDetail(lake, surveys) {
  const panel = document.getElementById("detail-panel");
  const mapsUrl = googleMapsUrl(lake);

  // Group by species
  const bySpecies = {};
  surveys.forEach((s) => {
    if (!bySpecies[s.species]) bySpecies[s.species] = [];
    bySpecies[s.species].push(s);
  });

  const speciesList = Object.keys(bySpecies)
    .filter((sp) => speciesLabel(sp) !== sp)
    .sort((a, b) => speciesLabel(a).localeCompare(speciesLabel(b)));

  // Default to the filtered species if active, else first alphabetically
  const defaultSpecies =
    (currentSpecies && bySpecies[currentSpecies])
      ? currentSpecies
      : (speciesList[0] || null);

  panel.innerHTML = `
    <div class="detail-header">
      <div>
        <h2>${escHtml(lake.name)}</h2>
        <div class="meta">${escHtml(lake.county)} County &bull; DOW #${lake.id}</div>
      </div>
      <div class="detail-links">
        <a class="maps-link" href="${mapsUrl}" target="_blank" rel="noopener">
          ${mapIcon()} Google Maps
        </a>
        <a class="maps-link" href="https://www.dnr.state.mn.us/lakefind/lake.html?id=${lake.id}" target="_blank" rel="noopener">
          ${dnrIcon()} MN DNR LakeFinder
        </a>
      </div>
    </div>

    ${speciesList.length === 0
      ? `<p class="no-surveys">No survey data available for this lake.</p>`
      : `
        <p class="section-title">Filter by species (${speciesList.length} surveyed)</p>
        <div class="species-tabs" id="species-tabs">
          ${speciesList.map((sp) => `
            <button class="species-tab ${sp === defaultSpecies ? "active" : ""}" data-species="${escAttr(sp)}">
              ${escHtml(speciesLabel(sp))}
            </button>
          `).join("")}
        </div>
        <div id="survey-table-container"></div>
      `
    }
  `;

  if (speciesList.length > 0) {
    renderSurveyTable(bySpecies[defaultSpecies] || []);

    document.getElementById("species-tabs").addEventListener("click", (e) => {
      const btn = e.target.closest(".species-tab");
      if (!btn) return;
      document.querySelectorAll(".species-tab").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      renderSurveyTable(bySpecies[btn.dataset.species] || []);
    });
  }
}

function renderSurveyTable(rows) {
  const sorted = [...rows].sort((a, b) => b.date.localeCompare(a.date));
  const container = document.getElementById("survey-table-container");
  if (!container) return;

  if (sorted.length === 0) {
    container.innerHTML = `<p class="no-surveys">No surveys for this species.</p>`;
    return;
  }

  container.innerHTML = `
    <p class="section-title">Survey history (${sorted.length} record${sorted.length !== 1 ? "s" : ""})</p>
    <div class="survey-table-wrap">
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>Type</th>
            <th>Gear</th>
            <th>Total Catch</th>
            <th>CPUE</th>
            <th>Avg Weight (lbs)</th>
            <th>Avg Size (in)</th>
            <th>Largest (in)</th>
          </tr>
        </thead>
        <tbody>
          ${sorted.map((r) => `
            <tr>
              <td>${r.date || "—"}</td>
              <td>${escHtml(r.type || "—")}</td>
              <td>${escHtml(r.gear || "—")}</td>
              <td>${r.total_catch ?? "—"}</td>
              <td>${r.cpue != null ? r.cpue.toFixed(2) : "—"}</td>
              <td>${r.avg_weight != null ? r.avg_weight.toFixed(2) : "—"}</td>
              <td>${r.avg_length != null ? r.avg_length.toFixed(1) : "—"}</td>
              <td>${r.max_length != null ? r.max_length : "—"}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `;
}

// ── Helpers ──
function speciesLabel(code) {
  return speciesNames[code] || code;
}

function googleMapsUrl(lake) {
  const query = encodeURIComponent(`${lake.name} Lake, ${lake.county} County, Minnesota`);
  return `https://www.google.com/maps/search/?api=1&query=${query}`;
}

function mapIcon() {
  return `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
    <path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/>
  </svg>`;
}

function dnrIcon() {
  return `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
    <circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/>
    <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>
  </svg>`;
}

function escHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;")
    .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function escAttr(str) {
  return String(str).replace(/"/g, "&quot;");
}

// ── Start ──
init();
