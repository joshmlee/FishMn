// ── State ──
let allLakes = [];
let speciesNames = {};
let surveyCache = {};   // county -> survey data (lazy loaded)
let currentCounty = "";
let currentSpecies = "";
let currentSort = "cpue";
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
  sortGroup.style.display = currentSpecies ? "" : "none";

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
    showLoadingBar(true);
    await loadAllCountySurveys(neededCounties, (done, total) => {
      updateLoadingBar(done, total);
    });
    showLoadingBar(false);
  }

  const ranked = rankLakes(lakes, currentSpecies, currentSort);
  renderLakeList(ranked.lakes, ranked.metricByDow);
}

function onSortChange() {
  currentSort = document.getElementById("sort-select").value;
  if (!currentSpecies) return;
  const lakes = filteredLakes();
  const ranked = rankLakes(lakes, currentSpecies, currentSort);
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

// For each lake, find the most recent survey row for the chosen species,
// then sort descending by the chosen metric. Returns only lakes that have data.
function rankLakes(lakes, speciesCode, sortField) {
  const metricByDow = {};

  for (const lake of lakes) {
    const countyData = surveyCache[lake.county] || {};
    const rows = (countyData[lake.id] || []).filter(
      (r) => r.species === speciesCode
    );
    if (rows.length === 0) continue;

    // Most recent survey for this species
    const latest = rows.reduce((best, r) =>
      r.date > best.date ? r : best
    );

    const val = latest[sortField];
    if (val == null) continue;

    metricByDow[lake.id] = { value: val, date: latest.date };
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
    avg_weight: "Avg wt",
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
    `;
    li.addEventListener("click", () => selectLake(lake, li));
    frag.appendChild(li);
  });
  ul.appendChild(frag);
}

function formatMetric(val, field) {
  if (val == null) return "—";
  if (field === "total_catch") return val.toLocaleString();
  return val.toFixed(2);
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

  const speciesList = Object.keys(bySpecies).sort((a, b) =>
    speciesLabel(a).localeCompare(speciesLabel(b))
  );

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
      <a class="maps-link" href="${mapsUrl}" target="_blank" rel="noopener">
        ${mapIcon()} View on Google Maps
      </a>
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
            <th>Total Weight (lbs)</th>
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
              <td>${r.total_weight != null ? r.total_weight.toFixed(1) : "—"}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `;
}

// ── Loading bar ──
function showLoadingBar(visible) {
  document.getElementById("loading-bar").classList.toggle("hidden", !visible);
}

function updateLoadingBar(done, total) {
  const pct = Math.round((done / total) * 100);
  document.querySelector(".loading-bar-inner").style.width = `${pct}%`;
  document.getElementById("loading-label").textContent =
    `Loading survey data… ${done}/${total} counties`;
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
