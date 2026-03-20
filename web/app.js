// ── State ──
let allLakes = [];
let speciesNames = {};
let surveyCache = {};       // county -> survey data (lazy loaded)
let currentCounty = "";
let currentLake = null;
let currentSpecies = null;

// ── Boot ──
async function init() {
  const [lakes, counties, species] = await Promise.all([
    fetchJSON("data/lakes.json"),
    fetchJSON("data/counties.json"),
    fetchJSON("data/species_names.json"),
  ]);

  allLakes = lakes;
  speciesNames = species;

  populateCountyDropdown(counties);
  renderLakeList(allLakes);

  document.getElementById("county-select").addEventListener("change", onCountyChange);
  document.getElementById("lake-search").addEventListener("input", onSearchInput);
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
    return {};
  }
}

// ── UI: County dropdown ──
function populateCountyDropdown(counties) {
  const sel = document.getElementById("county-select");
  counties.forEach((c) => {
    const opt = document.createElement("option");
    opt.value = c;
    opt.textContent = c;
    sel.appendChild(opt);
  });
}

// ── UI: Lake list ──
function renderLakeList(lakes) {
  const ul = document.getElementById("lake-list");
  const noResults = document.getElementById("no-results");
  const count = document.getElementById("results-count");

  ul.innerHTML = "";

  if (lakes.length === 0) {
    noResults.classList.remove("hidden");
    count.textContent = "";
    return;
  }

  noResults.classList.add("hidden");
  count.textContent = `${lakes.length.toLocaleString()} lake${lakes.length !== 1 ? "s" : ""}`;

  const frag = document.createDocumentFragment();
  lakes.forEach((lake) => {
    const li = document.createElement("li");
    li.dataset.id = lake.id;
    li.dataset.county = lake.county;
    li.innerHTML = `
      <div class="lake-name">${escHtml(lake.name)}</div>
      <div class="lake-county">${escHtml(lake.county)} County</div>
    `;
    li.addEventListener("click", () => selectLake(lake, li));
    frag.appendChild(li);
  });
  ul.appendChild(frag);
}

function filteredLakes() {
  const search = document.getElementById("lake-search").value.trim().toLowerCase();
  return allLakes.filter((l) => {
    const countyMatch = !currentCounty || l.county === currentCounty;
    const nameMatch = !search || l.name.toLowerCase().includes(search);
    return countyMatch && nameMatch;
  });
}

// ── Event handlers ──
function onCountyChange(e) {
  currentCounty = e.target.value;
  currentLake = null;
  currentSpecies = null;
  renderLakeList(filteredLakes());
  showDetailPlaceholder();
}

function onSearchInput() {
  renderLakeList(filteredLakes());
}

// ── Lake selection ──
async function selectLake(lake, li) {
  // Highlight selected
  document.querySelectorAll("#lake-list li").forEach((el) => el.classList.remove("active"));
  li.classList.add("active");

  currentLake = lake;
  currentSpecies = null;

  showDetailLoading(lake);

  const surveys = await loadCountySurveys(lake.county);
  const lakeSurveys = surveys[lake.id] || [];

  renderDetail(lake, lakeSurveys);
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

  // Group surveys by species, collect unique species
  const bySpecies = {};
  surveys.forEach((s) => {
    if (!bySpecies[s.species]) bySpecies[s.species] = [];
    bySpecies[s.species].push(s);
  });

  const speciesList = Object.keys(bySpecies).sort((a, b) =>
    speciesLabel(a).localeCompare(speciesLabel(b))
  );

  // Default to first species (alphabetical by common name)
  const defaultSpecies = speciesList[0] || null;
  currentSpecies = defaultSpecies;

  panel.innerHTML = `
    <div class="detail-header">
      <div>
        <h2>${escHtml(lake.name)}</h2>
        <div class="meta">${escHtml(lake.county)} County &bull; DOW #${lake.id}</div>
      </div>
      <a class="maps-link" href="${mapsUrl}" target="_blank" rel="noopener">
        ${mapIcon()}
        View on Google Maps
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
      currentSpecies = btn.dataset.species;
      renderSurveyTable(bySpecies[currentSpecies] || []);
    });
  }
}

function renderSurveyTable(rows) {
  // Sort by date descending
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
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function escAttr(str) {
  return String(str).replace(/"/g, "&quot;");
}

// ── Start ──
init();
