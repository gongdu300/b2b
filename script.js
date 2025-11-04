// ================== 공통 유틸 ==================
const $id = (s) => document.getElementById(s);
const esc = (s) => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

// ✅ 업로드 후 “원본→바뀐컬럼” 확인 모달
function openTranslateConfirm(tableName, mapping){
  const modal = document.getElementById('translateConfirm');
  // ✅ 모달을 body 직속으로 이동(부모 transform 영향 제거)
  if (modal && modal.parentElement !== document.body) {
    document.body.appendChild(modal);
  }

  const nameEl = document.getElementById('tc-tableName');
  const bodyEl = document.getElementById('tc-body');

  // 테이블명 표기
  nameEl.innerHTML = `생성된 테이블: <b>${esc(tableName || '(미정)')}</b>`;

  // 원본→최종 매핑 테이블 (변경된 것만)
  const changed = Object.entries(mapping || {}).filter(([oldH, newH]) => oldH !== newH);
  const rows = changed.length
    ? changed.map(([oldH, newH]) => `
        <tr>
          <td style="padding:6px 10px;"><code>${esc(oldH)}</code></td>
          <td style="padding:6px 10px; text-align:center;">→</td>
          <td style="padding:6px 10px;"><b>${esc(newH)}</b></td>
        </tr>`).join('')
    : `<tr><td colspan="3" style="padding:8px 10px; opacity:.8;">변경된 컬럼이 없습니다.</td></tr>`;

  bodyEl.innerHTML = `
    <div style="font-weight:600; margin-bottom:.25rem;">원본컬럼 → 바뀐컬럼</div>
    <table style="width:100%; border-collapse:collapse;">
      <thead>
        <tr>
          <th style="text-align:left; padding:6px 10px; border-bottom:1px solid var(--border-color,#334155);">원본컬럼</th>
          <th style="width:50px; border-bottom:1px solid var(--border-color,#334155);"></th>
          <th style="text-align:left; padding:6px 10px; border-bottom:1px solid var(--border-color,#334155);">바뀐컬럼</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;

  // 열기
  modal.style.display = 'flex';
  document.body.style.overflow = 'hidden'; // 배경 스크롤 잠금

  const onClose = ()=>{
    modal.style.display = 'none';
    document.body.style.overflow = '';      // 잠금 해제
  };
  document.getElementById('tc-ok')?.addEventListener('click', onClose, {once:true});
  modal.addEventListener('click', (e)=>{ if(e.target === modal) onClose(); }, {once:true});
}

// ✅ 전역 백엔드 URL (고정)
const BACKEND_BASE_URL = "http://127.0.0.1:8000";


// 전역 데이터 상태 (CSV든 DB든 여기에 채운다)
window.AppState = window.AppState || {
  name: null,        // 파일명 or 테이블명
  source: null,      // 'csv' | 'db'
  headers: [],       // 문자열 배열
  dataset: null      // [{col:val,...}, ...] 또는 [[],[]] 형태도 OK
};

async function fetchJSON(method, url, body) {
  const res = await fetch(url, {
    method,
    headers: body ? {'Content-Type': 'application/json'} : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const txt = await res.text();
  const js = txt ? JSON.parse(txt) : {};
  if (!res.ok) throw js;
  return js;
}

// [ADD] 인벤토리 컬럼 불러오기 (전역)
async function loadInventoryColumns(tableName){
  const whSel = document.getElementById('inv-warehouseColSel');
  const qtySel = document.getElementById('inv-qtyColSel');
  if (!whSel || !qtySel) return;

  whSel.innerHTML = '<option value="">(창고 컬럼 선택)</option>';
  qtySel.innerHTML = '<option value="">(재고 컬럼 선택)</option>';
  if (!tableName) return;

  try{
    const js = await fetchJSON('POST', `${BACKEND_BASE_URL}/table-columns`, { table_name: tableName });
    const cols = js.columns || [];

    const score = (s)=>{
      s = s.toLowerCase();
      if (/(wh|ware|창고)/.test(s)) return 3;                // 창고 후보 가중치
      if (/(qty|stock|재고|quantity|inv)/.test(s)) return 3; // 수량/재고 후보 가중치
      if (/(id|code|번호|no)/.test(s)) return 1;
      return 0;
    };
    const sorted = cols.slice().sort((a,b)=>score(b)-score(a));

    whSel.innerHTML += sorted.map(c=>`<option value="${c}">${c}</option>`).join('');
    qtySel.innerHTML += sorted.map(c=>`<option value="${c}">${c}</option>`).join('');

    const autoWh  = sorted.find(c=>/(wh|ware|창고)/i.test(c));
    const autoQty = sorted.find(c=>/(qty|stock|재고|quantity|inv)/i.test(c));
    if (autoWh)  whSel.value = autoWh;
    if (autoQty) qtySel.value = autoQty;

  }catch(e){
    whSel.innerHTML = '<option value="">(컬럼 불러오기 실패)</option>';
    qtySel.innerHTML = '<option value="">(컬럼 불러오기 실패)</option>';
  }
}

// 공용: 데이터 준비 여부
function hasDataset() {
  const ds = window.AppState?.dataset;
  const hs = window.AppState?.headers;
  return Array.isArray(hs) && hs.length > 0 && Array.isArray(ds); // 행 0개도 허용
}

// === 공용 로더: CSV/DB 어떤 소스든 dataLab 상태로 주입 ===
function rowsToObjects(hdrs, row2d){
  return row2d.map(r=>{
    const o={}; for(let i=0;i<hdrs.length;i++) o[hdrs[i]] = (r?.[i] ?? '');
    return o;
  });
}

function loadDataset({ name, source, hdrs, rows, rowsAreObjects=false }) {
  const objs = rowsAreObjects ? rows : rowsToObjects(hdrs, rows);

  // 1) 데이터 먼저 세팅
  window.DataLab.setData(name, source || 'db', hdrs, objs);

  // 2) 컬럼 개수만큼 미리보기 컬럼 최대값 자동 세팅
  const maxColsInput = document.getElementById('maxCols');
  if (maxColsInput) {
    const colCount = Array.isArray(hdrs) ? hdrs.length : 0;
    if (colCount > 0) {
      maxColsInput.value = colCount;      // 인풋 값 보이게
      maxColsInput.setAttribute('max', colCount); // 혹시 나중에 쓸 max도 같이
    }
  }

  // 3) 데이터 통계도 갱신하고 싶으면 여기서 트리거해도 됨
  // (지금 구조상 DataLab.setData 안에서 미리보기 다시 그릴 가능성이 큼)
}
window.DataLab = window.DataLab || {};
window.DataLab.loadDataset = loadDataset;

function gridToArrays() {
  const headers = window.AppState.headers.slice();                  // 현재 컬럼명(사용자 rename 반영)
  const rows = (window.AppState.dataset || []).map(row =>
    headers.map(h => row[h] ?? null)
  );
  return { headers, rows };
}
const saveNote = document.getElementById('saveNote');

document.getElementById('btnCreateNew')?.addEventListener('click', async ()=>{
  // 1) 새 테이블명 입력
  const newTable = prompt('새 테이블 이름을 입력하세요 (영문/숫자/언더스코어 추천):');
  if(!newTable) return;

  // 2) 그리드 -> CSV 문자열 생성
  const { headers, rows } = gridToArrays();
  if(!headers.length){ return alert('그리드에 데이터가 없습니다.'); }

  // CSV 만들기 (따옴표 이스케이프 포함)
  const headerLine = headers.map(h => `"${String(h).replace(/"/g,'""')}"`).join(',');
  let csv = headerLine + '\n';
  for(const r of rows){
    const line = headers.map((h)=> {
      const v = (r[headers.indexOf(h)] ?? r[h] ?? '');
      return `"${String(v).replace(/"/g,'""')}"`;
    }).join(',');
    csv += line + '\n';
  }

  // 3) /upload 로 파일처럼 전송 (파일명=새테이블명.csv)
  try{
    const blob = new Blob([csv], {type:'text/csv;charset=utf-8;'});
    const form = new FormData();
    form.append('file', blob, `${newTable}.csv`);

    const res = await fetch(`${BACKEND_BASE_URL}/upload`, { method:'POST', body: form });
    const js = await res.json();
    if(!res.ok) throw new Error(js.detail || 'upload failed');

    saveNote.textContent = `(${js.table_name}) 업로드 완료`;

    // ✅ 헤더 변환 결과 표시
    if (js.header_translation && Object.keys(js.header_translation).length > 0) {
      const list = Object.entries(js.header_translation)
        .filter(([oldH, newH]) => oldH !== newH)
        .map(([oldH, newH]) => `<li><code>${oldH}</code> → <b>${newH}</b></li>`)
        .join('');

      const msg = `
        <div class="translation-box" 
            style="margin-top:.5rem;padding:.5rem 1rem;
                    border:1px solid var(--border-color);
                    border-radius:8px;background:rgba(37,99,235,0.05)">
          <div style="font-weight:600;">자동 변환된 컬럼명</div>
          <ul style="margin:.25rem 0 0 .5rem;">${list}</ul>
        </div>
      `;
      const out = document.getElementById('up-healthOut');
      if (out) out.innerHTML = msg;

      // ✅ “원본→바뀐컬럼” 확인 모달 띄우기 + 테이블명 표기
      openTranslateConfirm(js.table_name, js.header_translation);
    }

    // 4) 결과 표시 + 셀렉트 갱신
    const action = js.table_action;
    const msg = (action==='created' || action==='replaced' || action==='merged')
      ? `(${js.table_name}) 생성 완료 · 적재 ${js.staged_rows??0} · 병합 ${js.merged_rows??0}`
      : (typeof action==='string' ? action : '완료');
    saveNote.textContent = msg;

    // 테이블 목록 새로고침 & 선택
    try{
      const data = await (await fetch(`${BACKEND_BASE_URL}/tables`)).json();
      const sel = document.getElementById('up-tableSelect');
      if (sel){
        sel.innerHTML = '<option value="">(테이블 선택)</option>' + 
          (data.tables||[]).map(t=>`<option value="${t}">${t}</option>`).join('');
        sel.value = js.table_name || newTable;
      }
    }catch(e){ /* noop */ }
  }catch(err){
    alert('새 테이블 만들기 실패: ' + (err.message || err));
    saveNote.textContent = '';
  }
});


// ================== 헤더/버튼 작은 인터랙션 ==================
(function bootstrapHeader(){
  const badge = document.querySelector('.notification-badge');
  let n = Number(badge?.textContent || 3);
  setInterval(() => {
    if (!badge) return;
    if (Math.random() > 0.75) {
      n += 1;
      badge.textContent = String(n);
      badge.style.animation = 'none';
      setTimeout(() => (badge.style.animation = 'blink 2s infinite'), 10);
    }
  }, 8000);

  document.querySelectorAll('.chart-controls .chart-btn').forEach(btn => {
    btn.addEventListener('click', function () {
      this.parentElement.querySelectorAll('.chart-btn').forEach(b => b.classList.remove('active'));
      this.classList.add('active');
    });
  });
})();

// ================== 섹션 전환 (HTML 구조 맞춤) ==================
(function sectionSwitching(){
  const navPrep      = document.getElementById('nav-prep');
  const prep         = document.getElementById('prep-section');
  const navForecast  = document.getElementById('nav-forecast');
  const navInventory = document.getElementById('nav-inventory');
  const forecast     = document.getElementById('forecast-section');
  const inventory    = document.getElementById('inventory-section');

  function show(name){
    const pairs = [
      {nav: navPrep,      sec: prep,      key: 'prep'},
      {nav: navForecast,  sec: forecast,  key: 'forecast'},
      {nav: navInventory, sec: inventory, key: 'inventory'},
    ].filter(p => p.nav && p.sec);

    const target = pairs.find(p => p.key === name) || pairs[0];
    if (!target) return;

    pairs.forEach(p => { p.sec.style.display = 'none'; p.nav.classList.remove('active'); });
    target.sec.style.display = 'block';
    target.nav.classList.add('active');

    try { localStorage.setItem('TAB', target.key); } catch {}
  }
  // (예전엔 클릭 바인딩 없음)
  // ✅ 클릭 바인딩 추가
  navPrep?.addEventListener('click', () => show('prep'));
  navForecast?.addEventListener('click', () => show('forecast'));
  navInventory?.addEventListener('click', () => show('inventory'));
  // 마지막 탭 기억
  const saved = (()=>{ try { return localStorage.getItem('TAB'); } catch { return null; } })();
  show(saved || 'prep');
})();

// ================== 업로더(백엔드 연동) ==================
(function uploader(){
 
  const BASE = () => BACKEND_BASE_URL;
  const saveBase = () => {};
  const showJSON = (target, data) => {
    const el = typeof target === 'string' ? $id(target) : target;
    if(!el) return console.warn('showJSON target not found:', target);
    el.textContent = (typeof data==='string') ? data : JSON.stringify(data, null, 2);
  };

  function renderUploadsTable(store){
    const table = $id('up-uploadsTable');
    if(!table){ return; }

    // 비어있으면 초기화만
    if(!store || typeof store !== 'object' || Object.keys(store).length === 0){
      table.querySelector('thead').innerHTML = '<tr><th>파일</th><th>컬럼수</th><th>업로드 시각(UTC)</th><th>테이블</th><th>액션</th><th>적재/병합</th><th>헤더</th></tr>';
      table.querySelector('tbody').innerHTML = '<tr><td colspan="7" style="text-align:center;">데이터가 없습니다</td></tr>';
      return;
    }

    const rows = [];
    const keys = Object.keys(store);

    // 정렬(최근 업로드 우선)
    keys.sort((a,b)=>{
      const da = store[a]?.uploaded_at || '';
      const db = store[b]?.uploaded_at || '';
      return (db>da) ? 1 : (db<da ? -1 : 0);
    });

    const TH = `
      <tr>
        <th>파일명</th>
        <th>컬럼수</th>
        <th>업로드 시각(UTC)</th>
        <th>테이블</th>
        <th>액션</th>
        <th>적재/병합</th>
        <th>헤더</th>
      </tr>`;

    for(const id of keys){
      const it = store[id] || {};
      const name   = esc(it.filename);
      const ncols  = Number(it.num_columns ?? 0);
      const when   = esc(it.uploaded_at || '');
      const tname  = esc(it.table_name || '');
      const action = esc(it.table_action || '');
      const staged = it.staged_rows!=null ? Number(it.staged_rows) : null;
      const merged = it.merged_rows!=null ? Number(it.merged_rows) : null;

      // 헤더 툴팁 & 버튼
      const headers = Array.isArray(it.headers) ? it.headers : [];
      const headerTip = headers.map(h=>`• ${h}`).join('\n');
      const headerBtn = headers.length
        ? `<button class="pp-btn" title="${esc(headerTip)}" onclick="alert('${esc(headerTip)}')">보기</button>`
        : `<span style="opacity:.6;">(없음)</span>`;

      rows.push(`
        <tr>
          <td><code>${name}</code><br><small style="opacity:.7;">${esc(id)}</small></td>
          <td style="text-align:right;">${ncols}</td>
          <td><small>${when}</small></td>
          <td>${tname ? `<code>${tname}</code>` : '<span style="opacity:.6;">-</span>'}</td>
          <td>${action || '<span style="opacity:.6;">-</span>'}</td>
          <td>${(staged!=null || merged!=null) ? `적재 ${staged??0} · 병합 ${merged??0}` : '<span style="opacity:.6;">-</span>'}</td>
          <td>${headerBtn}</td>
        </tr>
      `);
    }

    table.querySelector('thead').innerHTML = TH;
    table.querySelector('tbody').innerHTML = rows.join('');
  }

  // 아주 단순 CSV 파서 (따옴표 처리 O)
  function parseCsvLine(line) {
    const out = [];
    let cur = '';
    let inQ = false;

    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"' ) {
        if (inQ && line[i+1] === '"') {
          // 이스케이프된 따옴표
          cur += '"';
          i++;
        } else {
          inQ = !inQ;
        }
      } else if (ch === ',' && !inQ) {
        out.push(cur);
        cur = '';
      } else {
        cur += ch;
      }
    }
    out.push(cur);
    return out;
  }

  function onPickFile(){
    const f = $id('up-fileInput').files[0];
    $id('up-fileNameLabel').textContent = f ? f.name : '';

    // 👇 추가: 파일을 바로 미리보기로 뿌리기
    if (f) {
      const reader = new FileReader();
      reader.onload = (e) => {
        const text = e.target.result || '';
        // 간단 CSV 파서 (첫 줄: 헤더, 나머지: rows)
        const lines = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n').filter(l => l.trim() !== '');
        if (!lines.length) return;

        // 1) 헤더
        const headerLine = lines[0];
        const headers = parseCsvLine(headerLine);

        // 2) 행들
        const rows = [];
        const MAX_PREVIEW = 100;       // 데이터랩이랑 맞춰서 100까지만
        for (let i = 1; i < lines.length && i <= MAX_PREVIEW; i++) {
          const cols = parseCsvLine(lines[i]);
          rows.push(cols);
        }

        // 3) 데이터랩으로 주입 (기존 공용 함수 재사용)
        loadDataset({
          name: f.name,
          source: 'csv',
          hdrs: headers,
          rows: rows,
          rowsAreObjects: false
        });
      };
      reader.readAsText(f, 'utf-8');
    }
  }


  async function callJSON(method, url, bodyObj){
    const res = await fetch(url, {
      method,
      headers: bodyObj ? {'Content-Type':'application/json'} : undefined,
      body: bodyObj ? JSON.stringify(bodyObj) : undefined
    });
    const text = await res.text();
    try{
      const json = text ? JSON.parse(text) : {};
      if(!res.ok) throw {status:res.status, json};
      return json;
    }catch(e){
      if(e && e.status) throw e;
      throw {status:res.status, json: text || '(no body)'};
    }
  }

  async function fetchTables(){
    try{
      const data = await callJSON('GET', `${BASE()}/tables`);
      const sel = $id('up-tableSelect');
      sel.innerHTML = '';
      const opt0 = document.createElement('option');
      opt0.value=''; opt0.textContent='(테이블 선택)';
      sel.appendChild(opt0);
      (data.tables||[]).forEach(t=>{
        const o = document.createElement('option');
        o.value=t; o.textContent=t;
        sel.appendChild(o);
      });
    }catch(e){
      const sel = $id('up-tableSelect');
      sel.innerHTML='';
      const o=document.createElement('option');
      o.value=''; o.textContent='(불러오기 실패)';
      sel.appendChild(o);
    }
  }

  async function fetchTables2(){
      try{
      const data = await callJSON('GET', `${BASE()}/tables`);

      // 공통 옵션 HTML 생성
      const makeOptionsHTML = (tables=[]) => {
        const first = `<option value="">(테이블 선택)</option>`;
        const rest  = tables.map(t => `<option value="${t}">${t}</option>`).join('');
        return first + rest;
      };

      // 업로더 셀렉트
      const upSel = $id('up-tableSelect');
      if (upSel) upSel.innerHTML = makeOptionsHTML(data.tables || []);

      // ✅ 인벤토리 셀렉트도 동일하게 채움
      const invSel = $id('inv-tableSelect');
      if (invSel) invSel.innerHTML = makeOptionsHTML(data.tables || []);

    }catch(e){
      const failHTML = `<option value="">(불러오기 실패)</option>`;
      const upSel  = $id('up-tableSelect');
      const invSel = $id('inv-tableSelect');
      if (upSel)  upSel.innerHTML  = failHTML;
      if (invSel) invSel.innerHTML = failHTML;
    }
  }

  async function health(){
    saveBase();
    try{
      const json = await callJSON('GET', `${BASE()}/_health`);
      $id('up-healthText').textContent = `engine: ${json.engine?'true':'false'} · startup_ok: ${json.startup_ok?'true':'false'}`;
      showJSON('up-healthOut', json);
      await fetchTables2();
    }catch(e){
      $id('up-healthText').textContent = 'engine: err · startup_ok: err';
      showJSON('up-healthOut', e.json);
    }
  }

  async function upload(){
    saveBase();
    const f = $id('up-fileInput').files[0];
    if(!f){ showJSON('up-uploadOut', 'CSV 파일을 선택하세요.'); return; }

    // ✅ 추가: 진행도 DOM
    const box  = $id('up-progress');
    const bar  = $id('up-progress-bar');
    const text = $id('up-progress-text');
    const btn  = $id('up-btnUpload');

    // 시작 상태
    if (box) box.style.display = 'block';
    if (bar) bar.style.width = '20%';
    if (text) text.textContent = 'CSV 업로드 중... (1/3)';
    if (btn) btn.disabled = true;

    // ✅ 이 순서가 정답
    const form = new FormData();
    form.append('file', f);

    try{
      const res  = await fetch(`${BASE()}/upload`, { method:'POST', body:form });

      // 네트워크 구간 끝 → DB 저장/머지 중이라고 표시
      if (bar) bar.style.width = '65%';
      if (text) text.textContent = 'DB에 적재/병합 중... (2/3)';

      const json = await res.json();
      if(!res.ok) throw {status:res.status, json};
      showJSON('up-uploadOut', json);

      // ✅ 헤더 변환 결과 확인 및 알림창 띄우기
      if (json.header_translation && Object.keys(json.header_translation).length > 0) {
        const changed = Object.entries(json.header_translation)
          .filter(([oldH, newH]) => oldH !== newH);

        if (changed.length) {
          const list = changed.map(([oldH, newH]) =>
            `<li><code>${esc(oldH)}</code> → <b>${esc(newH)}</b></li>`).join('');

          const msg = `
            <div class="translation-box"
                style="margin-top:.5rem;padding:.5rem 1rem;
                        border:1px solid var(--border-color);
                        border-radius:8px;background:rgba(37,99,235,0.05)">
              <div style="font-weight:600;">자동 변환된 컬럼명</div>
              <ul style="margin:.25rem 0 0 .5rem;">${list}</ul>
            </div>`;
          const out = document.getElementById('up-translateOut') || document.getElementById('up-healthOut');
          if (out) out.innerHTML = msg;

          // ✅ 테이블 이름과 함께 확인 모달 띄우기
          openTranslateConfirm(json.table_name, json.header_translation);
        }
      }

      // (옵션) 경고도 표시
      if (Array.isArray(json.header_warnings) && json.header_warnings.length) {
        const warnHtml = `
          <div class="translation-warn"
              style="margin-top:.5rem;padding:.5rem 1rem;
                      border:1px dashed var(--border-color);
                      border-radius:8px;background:rgba(234,179,8,0.08)">
            <div style="font-weight:600;">헤더 정제 경고</div>
            <ul style="margin:.25rem 0 0 .5rem;">
              ${json.header_warnings.map(w => `<li>${esc(w)}</li>`).join('')}
            </ul>
          </div>
        `;
        const out = document.getElementById('up-translateOut') || document.getElementById('up-healthOut');
        if (out) out.insertAdjacentHTML('beforeend', warnHtml);
      }

      const action = json.table_action;
      let msg = action==='replaced' ? '덮어쓰기 완료!' :
        (action==='created' || action==='merged') ? '추가 완료!' :
        (typeof action==='string' && action.startsWith('error:')) ? action : '완료';
      const info=[];

      if(json.table_name) info.push(`테이블: ${json.table_name}`);
      if(json.staged_rows!=null) info.push(`적재: ${json.staged_rows}행`);
      if(json.merged_rows!=null) info.push(`병합: ${json.merged_rows}행`);
      $id('up-uploadNote').textContent = `(${msg}) ` + info.join(' · ');

      if(json.table_name){
        await fetchTables2();
        $id('up-tableSelect').value = json.table_name;

        // ✅ 인벤토리 테이블 셀렉트도 맞춰주고 컬럼 자동 로드
        const invSel = document.getElementById('inv-tableSelect');
        if (invSel){
          invSel.value = json.table_name;
          loadInventoryColumns(json.table_name);
        }
      }

      // 완료
      if (bar) bar.style.width = '100%';
      if (text) text.textContent = '완료되었습니다. (3/3)';
    } catch (e) {
      showJSON('up-uploadOut', e.json || e);
      if (bar) bar.style.width = '100%';
      if (text) text.textContent = '업로드 실패';
    } finally {
      if (btn) btn.disabled = false;
      if (box) {
        setTimeout(() => {
          box.style.display = 'none';
          if (bar) bar.style.width = '0';
        }, 1200);
      }
    }
  }

  async function stats(){
    saveBase();
    const t = $id('up-tableSelect').value.trim();
    if(!t){ showJSON('up-statsOut','테이블을 선택하세요.'); return; }
    try{
      const json = await callJSON('POST', `${BASE()}/stats`, {table_name: t});
      showJSON('up-statsOut', json);
    }catch(e){
      showJSON('up-statsOut', e.json);
    }
  }

  async function debugUploads(){
    saveBase();
    try{
      const json = await callJSON('GET', `${BASE()}/_debug/uploads`);
      // 표 렌더
      renderUploadsTable(json);
      // 원본 JSON은 details 안에 보관
      showJSON('up-debugJson', json);
    }catch(e){
      // 에러도 표 대신 JSON으로만 보여줍니다.
      $id('up-uploadsTable').querySelector('thead').innerHTML = '';
      $id('up-uploadsTable').querySelector('tbody').innerHTML = '';
      showJSON('up-debugJson', e.json);
    }
  }



  // 바인딩
  $id('up-fileInput') ?.addEventListener('change', onPickFile);
  $id('up-btnHealth') ?.addEventListener('click', health);
  $id('up-btnUpload') ?.addEventListener('click', upload);
  $id('up-btnStats')  ?.addEventListener('click', stats);
  $id('up-btnUploads')?.addEventListener('click', debugUploads);

  // ✅ 미리보기 행 최대값 100으로 강제
  document.addEventListener('DOMContentLoaded', () => {
    const maxRowsInput = document.getElementById('maxRows');
    if (maxRowsInput) {
      maxRowsInput.addEventListener('input', (e) => {
        const val = parseInt(e.target.value, 10);
        if (val > 100) {
          e.target.value = 100;
        } else if (val < 1) {
          e.target.value = 1;
        }
      });
    }

    const maxColsInput = document.getElementById('maxCols');
    if (maxColsInput) {
      maxColsInput.addEventListener('input', (e) => {
        const current = parseInt(e.target.value, 10);
        // 우리가 loadDataset에서 넣어놓은 max 속성 읽기
        const maxAllowed = parseInt(e.target.getAttribute('max'), 10);

        // max가 없다면 그냥 리턴
        if (Number.isNaN(maxAllowed)) return;

        // 범위 체크
        if (current > maxAllowed) {
          e.target.value = maxAllowed;
        } else if (current < 1) {
          e.target.value = 1;
        }
      });
    }
  });

  // 최초 1회
  health();
})();



// ====== [PATCH] Type inference helpers ======
function countMaxDecimals(values, maxSample=500){
  let d = 0, seen=0;
  for (let i=0; i<values.length && seen<maxSample; i++){
    const v = values[i];
    if (v === '' || v === null || v === undefined) continue;
    const m = String(v).match(/\.(\d+)/);
    if (m) d = Math.max(d, m[1].length);
    seen++;
  }
  return Math.min(d, 6);
}
function gcd(a,b){ return b ? gcd(b, a % b) : Math.abs(a); }
function inferDiscreteStepAndCoverage(nums, decimals){
  if (nums.length < 5) return {step:null, coverage:null, states:null};
  const scale = Math.pow(10, decimals);
  const ints = nums.map(n => Math.round(n*scale)).sort((a,b)=>a-b);
  const diffs = [];
  for (let i=1;i<ints.length;i++){ const d = ints[i]-ints[i-1]; if (d>0) diffs.push(d); }
  if (!diffs.length) return {step:null, coverage:null, states:1};
  let g = diffs[0];
  for (let i=1;i<diffs.length;i++){ g = gcd(g, diffs[i]); if (g===1) break; }
  const step = g/scale;
  const min = Math.min(...ints), max = Math.max(...ints);
  const possibleStates = step > 0 ? Math.floor((max - min)/g) + 1 : null;
  const uniq = new Set(ints).size;
  const coverage = (possibleStates && possibleStates>0) ? (uniq / possibleStates) : null;
  return { step, coverage, states: possibleStates };
}
function looksLikeId(str){ return /^[A-Za-z_]+[\d]{2,}$/.test(str); }
function mostValuesLookLikeId(values){
  let cnt=0, seen=0; const n=Math.min(values.length,300);
  for(let i=0;i<n;i++){ const s=String(values[i]??'').trim(); if(!s) continue; seen++; if(looksLikeId(s)) cnt++; }
  return seen>0 && (cnt/seen)>=0.7;
}

function isLowCardinality(uniqueCount, nonMissingCount, { ratio=0.10 } = {}) {
  if (nonMissingCount === 0) return false;
  const r = uniqueCount / nonMissingCount;
  return r <= ratio;
}

// tool_fileInfo() 내부에서 예시 구하는 부분만 교체
function samplePrettyForColumn(colVals, type0){
  // 숫자형 판별
  const nums = colVals.map(Number).filter(Number.isFinite);
  if (type0 === 'number' && nums.length) {
    const nonZero = nums.filter(n => n !== 0);
    const arr = (nonZero.length ? nonZero : nums).slice().sort((a,b)=>a-b);
    const pick = (k)=> arr[Math.min(arr.length-1, Math.max(0, Math.floor(k)))];
    const min = pick(0);
    const med = pick((arr.length-1)/2);
    const max = pick(arr.length-1);
    return `${min}, ${med}, ${max}`;
  }

  if (type0 === 'category') {
    const s = new Set();
    for (const v of colVals) if (v!=='' && v!=null) s.add(String(v));
    const uniqArr = [...s].sort((a,b)=>String(a).localeCompare(String(b),'ko',{numeric:true,sensitivity:'base'}));
    // 너무 길면 앞 30개만
    const head = uniqArr.slice(0, 30);
    const tail = uniqArr.length > 30 ? ' …' : '';
    return `[${head.join(', ')}${tail}]`;
  }

  // 문자열 등: 앞쪽에서 다른 값 3개 수집(0/빈값 제외)
  const distinct = [];
  const seen = new Set();
  for (const vRaw of colVals) {
    const v = String(vRaw ?? '').trim();
    if (!v || seen.has(v)) continue;
    seen.add(v);
    distinct.push(v);
    if (distinct.length >= 3) break;
  }
  return distinct.join(', ');
}



function inferColType_generic(data, col){
  let nNum=0, nDate=0, nNonEmpty=0;
  const maxCheck = Math.min(200, data.length);
  for (let i=0;i<maxCheck;i++){
    const raw = data[i]?.[col];
    const v = (raw===undefined || raw===null) ? '' : String(raw).trim();
    if (v==='') continue;
    nNonEmpty++;
    const num = parseFloat(v);
    if (Number.isFinite(num) && /^-?\d+(\.\d+)?$/.test(v)) nNum++;
    else if (!isNaN(Date.parse(v))) nDate++;
  }
  if (nNonEmpty===0) return 'string';
  if (nDate/nNonEmpty >= 0.8) return 'date';

  const colVals = data.map(r=>r[col]).map(x => (x==null?'':String(x).trim())).filter(x=>x!=='');
  const uniques = new Set(colVals).size;
  const uniqueRatio = colVals.length ? (uniques/colVals.length) : 1;

  if (nNum/nNonEmpty >= 0.8){
    const nums = colVals.map(v=>parseFloat(v)).filter(Number.isFinite);
    const decs = countMaxDecimals(colVals);
    const { step, coverage, states } = inferDiscreteStepAndCoverage(nums, decs);

    // ➊ “반복 거의 없음 = 연속형” 규칙 추가
    //    - 숫자 고유값 비율(숫자기준)과 최빈값 비율을 계산
    const numKey = n => (decs > 0 ? Number(n.toFixed(decs)) : Math.round(n)); // 같은 값으로 버킷팅
    const counts = new Map();
    for (const n of nums) counts.set(numKey(n), (counts.get(numKey(n))||0)+1);
    const uniquesNums = counts.size;
    const uniqueNumRatio = nums.length ? (uniquesNums / nums.length) : 1;        // 고유값 비율(숫자 기준)
    const maxFreq = nums.length ? Math.max(...counts.values()) : 0;
    const maxFreqRatio = nums.length ? (maxFreq / nums.length) : 0;              // 최빈값 점유율
    const repeatRatio = 1 - uniqueNumRatio;                                      // 중복 비율

    // 🔒 임계값(원하면 조정):
    const RARE_REPEAT = 0.05;   // 최빈값이 5% 미만이면 “반복 거의 없음”
    const LOW_DUP    = 0.05;    // 전체 중복비율 5% 미만이면 “반복 거의 없음”

    // 👉 반복 거의 없으면 무조건 number(카테고리 아님)
    if (maxFreqRatio < RARE_REPEAT || repeatRatio < LOW_DUP) return 'number';

    // ➋ 기존 로직
    let spread=null;
    if (nums.length>1){
      const mean = nums.reduce((a,b)=>a+b,0)/nums.length;
      const std  = Math.sqrt(nums.reduce((a,n)=>a+(n-mean)**2,0)/nums.length);
      spread = std / (Math.abs(mean)+1e-9);
    }
    const isIntegerish = (decs===0);

    // (비율만 반영) 정수형 & 고유값비율 낮으면 카테고리
    const integerCategorical = isIntegerish && (uniqueRatio <= 0.10);
    const looksDiscreteGrid  = step && states && states<=256 && coverage!=null && coverage<0.6;
    const looksContinuous    = spread!=null && spread>0.20;

    if (integerCategorical || looksDiscreteGrid) return 'category';
    if (looksContinuous) return 'number';

    // 숫자 케이스 저카디널리티(비율 기준)
    return isLowCardinality(uniques, colVals.length, { ratio: 0.30 })
      ? 'category' : 'number';
  }

    // 문자열 케이스
    const idLike = mostValuesLookLikeId(colVals);
    if (idLike) {
      const uniqueRatio = colVals.length ? (uniques / colVals.length) : 1;
      // ID처럼 보이고 거의 유일(값이 대부분 다름) → id로 확정
      if (uniqueRatio >= 0.8) return 'id';
    }

    // 고유값이 적으면 category, 아니면 string
    return isLowCardinality(uniques, colVals.length, { ratio: 0.25 })
      ? 'category'
      : 'string';
  }

// --- ✅ 테이블 미리보기 & CSV 다운로드 기능 추가 ---
async function fetchTablePreview(tableName, limit = 100) {
  const url = `${BACKEND_BASE_URL}/table/${encodeURIComponent(tableName)}/preview?limit=${limit}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}


$id('btnPreviewTable')?.addEventListener('click', async () => {
  const t = $id('up-tableSelect')?.value;
  if (!t) { alert('테이블을 선택하세요.'); return; }
  try {
    const res = await fetchTablePreview(t);
    // ✅ 여기서 바로 공용 로더로 넘긴다
    loadDataset({
      name: t,
      source: 'db',
      hdrs: res.columns,
      rows: res.rows,          // 2D 배열
      rowsAreObjects: false
    });
  } catch (err) {
    alert('미리보기에 실패했습니다: ' + err);
  }
});


// ✅ 다운로드 버튼
$id('btnDownloadTable')?.addEventListener('click', () => {
  const t = $id('up-tableSelect')?.value;
  if (!t) { alert('테이블을 선택하세요.'); return; }
  const a = document.createElement('a');
  a.href = `${BACKEND_BASE_URL}/table/${encodeURIComponent(t)}/download`;
  a.download = `${t}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
});


// ================== 데이터랩(로컬 CSV 미리보기 & 전처리 도구) ==================
(function dataLab(){
  const dl = {
    fileInput:   $id('up-fileInput'),
    downloadBtn: $id('downloadProcessed'),
    maxRows:     $id('maxRows'),
    maxCols:     $id('maxCols'),
    applyPrev:   $id('applyPreview'),
    dataStats:   $id('dataStats'),
    dataSummary: $id('dataSummary'),
    table:       $id('previewTable'),
    thead:       document.querySelector('#previewTable thead'),
    tbody:       document.querySelector('#previewTable tbody'),
    empty:       $id('emptyState'),
    tools:       document.querySelectorAll('.dl-toolbar .pp-btn'),
    toolArea:    $id('toolFormArea'),
  };

  let rawCSV = '';
  let parsed = { headers: [], rows: [] };
  let headers = [];
  let data = [];
  let rawHeaders = [];
  const previewState = { rowIdxs:null, colIdxs:null, maxRows:20, maxCols:8 };
  // === 외부에서 DB 데이터를 주입하기 위한 공개 API ===
  window.DataLab = window.DataLab || {};
  window.DataLab.setData = (name, source, hdrs, rowsObjs) => {
    headers    = Array.isArray(hdrs) ? hdrs.slice() : [];
    rawHeaders = headers.slice();
    data       = Array.isArray(rowsObjs) ? rowsObjs.slice() : [];
    // 미리보기 초기화 및 렌더
    previewState.rowIdxs = null;
    previewState.colIdxs = null;
    updateStats();
    renderPreview();
    dl.downloadBtn.disabled = false;
    dl.toolArea.innerHTML = '';
    // (선택) 앱 상태 동기화
    window.AppState.name    = name || null;
    window.AppState.source  = source || 'db';
    window.AppState.headers = headers.slice();
    window.AppState.dataset = data.map(r => ({...r}));
  };

  // ▼ dataLab() IIFE 내부 어딘가(렌더/전처리 함수들 정의된 아래쪽)에 추가
  // function gridToArrays() {
  //   const headersNow = (window.AppState.headers || headers).slice();
  //   const rowsNow = (window.AppState.dataset || data).map(row =>
  //     headersNow.map(h => row[h] ?? null)
  //   );
  //   return { headers: headersNow, rows: rowsNow };
  // }

  function _applyToAppState(headersLocal, dataLocal){
    window.AppState.headers = headersLocal.slice();
    window.AppState.dataset = dataLocal.map(r => ({...r}));
  }
  // 추가 수정 --------
  function applyPreview(){
    // 행/열 최대값 읽기
    const mr = parseInt(dl.maxRows.value) || 20;
    const mc = parseInt(dl.maxCols.value) || 8;
    previewState.maxRows = mr;
    previewState.maxCols = mc;

    // 데이터가 비어있으면 스킵
    if(!data.length || !headers.length){ 
      dl.dataStats.textContent = '데이터 없음';
      return;
    }

    // 무작위 샘플링 또는 앞부분 고정
    previewState.rowIdxs = sampleIndices(data.length, mr);
    previewState.colIdxs = Array.from({length: Math.min(mc, headers.length)}, (_,i)=>i);
    
    renderPreview(); // 화면 갱신
  }

  // CSV 파서
  function csvParse(text){
    const rows=[]; let row=[], cur='', inQ=false;
    for(let i=0;i<text.length;i++){
      const ch=text[i], nx=text[i+1];
      if(inQ){
        if(ch==='"' && nx==='"'){ cur+='"'; i++; }
        else if(ch==='"'){ inQ=false; }
        else { cur+=ch; }
      }else{
        if(ch==='"') inQ=true;
        else if(ch===','){ row.push(cur); cur=''; }
        else if(ch==='\n'){ row.push(cur); rows.push(row); row=[]; cur=''; }
        else if(ch==='\r'){ /* ignore */ }
        else { cur+=ch; }
      }
    }
    row.push(cur); rows.push(row);
    while(rows.length && rows[rows.length-1].every(c => c === '')) rows.pop();
    return rows;
  }

  function toObjects(rows){
    if(!rows.length) return {headers:[], rows:[], rawHeaders:[]};
    const rawHdrs = rows[0].map(h => (h && h.trim()) ? h.trim() : 'col_' + Math.random().toString(36).slice(2,7));
    const seen = new Map();
    const hdrs = rawHdrs.map(h=>{
      const n=(seen.get(h)||0)+1; seen.set(h,n);
      return n===1 ? h : `${h}__${n}`;
    });
    const objs=[];
    for(let r=1;r<rows.length;r++){
      const o={};
      for(let c=0;c<hdrs.length;c++) o[hdrs[c]] = rows[r][c] ?? '';
      objs.push(o);
    }
    return { headers: hdrs, rows: objs, rawHeaders: rawHdrs };
  }

  function updateStats(){
    dl.dataStats.textContent = `총 ${data.length.toLocaleString()} rows × ${headers.length.toLocaleString()} cols`;
  }

  function sampleIndices(n,k){
    if(k>=n) return Array.from({length:n},(_,i)=>i);
    const set=new Set(); while(set.size<k) set.add(Math.floor(Math.random()*n));
    return [...set];
  }

  function renderPreview(markInfo = null){
    if(!headers.length){
      dl.thead.innerHTML=''; dl.tbody.innerHTML=''; dl.empty.style.display='block'; return;
    }
    dl.empty.style.display='none';

    const rowIdxs = (previewState.rowIdxs ?? data.map((_,i)=>i))
      .filter(i=>i>=0 && i<data.length).sort((a,b)=>a-b);
    const colIdxs = (previewState.colIdxs ?? headers.map((_,i)=>i))
      .filter(i=>i>=0 && i<headers.length);

    const H = colIdxs.map(i=>headers[i]);
    const headHtml = H.map(h=>{
      const t = inferColType_generic(data, h);
      const badge =
        t==='number'   ? '<span class="col-badge">#</span>' :
        t==='date'     ? '<span class="col-badge">🗓</span>' :
        t==='category' ? '<span class="col-badge">🏷</span>' :
                        '<span class="col-badge">Aa</span>';
      return `<th>${esc(h)} ${badge}</th>`;
    }).join('');
    dl.thead.innerHTML = `<tr>${headHtml}</tr>`;

    const rowsHtml = rowIdxs.map(r=>{
      const row = data[r];
      const tds = H.map(h=>{
        const isMarked = markInfo && h===markInfo.col && markInfo.indices && markInfo.indices.has(r);
        const cls = (isMarked ? 'outlier-cell ' : '') + 'readonly-cell';
        const val = String(row[h] ?? '');
        // 🔒 contenteditable 제거 → 완전 읽기 전용
        return `<td class="${cls}" data-row="${r}" data-col="${esc(h)}">${esc(val)}</td>`;
      }).join('');

      return `<tr>${tds}</tr>`;
    }).join('');
    dl.tbody.innerHTML = rowsHtml;

    // 🔥 편집 이벤트 → data에 즉시 반영
    dl.tbody.querySelectorAll('td[contenteditable="true"]').forEach(td=>{
      td.addEventListener('blur', (e)=>{
        const r = Number(td.getAttribute('data-row'));
        const h = td.getAttribute('data-col');
        // contenteditable은 HTML이 들어올 수 있으니 textContent 사용
        const newVal = td.textContent ?? '';
        if (data[r]) data[r][h] = newVal;

        // AppState 동기화
      _applyToAppState(headers, data);
      });
    });
  }

// function gridToArrays() {
//   const headers = (window.AppState.headers || []).slice();
//   const rows = (window.AppState.dataset || []).map(row => headers.map(h => row[h] ?? null));
//   return { headers, rows };
// }


  // ===== 전처리 도구들 =====
  function buildToolCard(title, inner, onApply){
    dl.toolArea.innerHTML = `
      <div class="tool-card">
        <h3>${title}</h3>
        <div class="tool-inline">${inner}</div>
        <div class="tool-actions">
          <button class="chart-btn" id="toolApply">확인</button>
          <button class="chart-btn" id="toolCancel" style="background:rgba(239,68,68,.15);border-color:rgba(239,68,68,.35)">취소</button>
        </div>
      </div>`;
    $id('toolCancel').onclick = ()=> dl.toolArea.innerHTML='';
    $id('toolApply').onclick  = onApply;
  }

  function tool_dropCols(){
    const options = headers.map((h,i)=>`<option value="${i}">${esc(h)}</option>`).join('');
    buildToolCard('🧹 컬럼 삭제', `
      <div class="dl-group" style="min-width:260px;">
        <label>삭제할 컬럼(여러 개 선택 가능)</label>
        <select id="dropColsSelect" multiple size="${Math.min(8, Math.max(3, headers.length))}" style="min-width:260px; max-width:420px;">
          ${options}
        </select>
        <small style="color:var(--text-secondary);">Ctrl/⌘ 또는 Shift로 다중 선택</small>
      </div>`, ()=>{
      const sel = $id('dropColsSelect');
      const idxs = Array.from(sel.selectedOptions).map(o=>parseInt(o.value,10)).sort((a,b)=>b-a);
      if(!idxs.length) return;

      const keepIdx = headers.map((_,i)=>i).filter(i=>!idxs.includes(i));
      const newHeaders = keepIdx.map(i=>headers[i]);
      data = data.map(row=>{ const o={}; newHeaders.forEach(h=>o[h]=row[h]); return o; });
      headers = newHeaders;

      if(previewState.colIdxs){
        previewState.colIdxs = previewState.colIdxs
          .filter(i=>!idxs.includes(i))
          .map(i=> i - idxs.filter(x=>x<i).length)
          .filter(i=> i>=0 && i<headers.length);
      }
      updateStats(); renderPreview(); dl.toolArea.innerHTML='';
      _applyToAppState(headers, data);
    });
  }

  function tool_renameCol(){
    const options = headers.map(h=>`<option value="${esc(h)}">${esc(h)}</option>`).join('');
    buildToolCard('✏️ 컬럼 이름 변경', `
      <div class="dl-group"><label>대상 컬럼</label><select id="renameFrom">${options}</select></div>
      <div class="dl-group"><label>새 이름</label><input id="renameTo" type="text" placeholder="새 컬럼명"/></div>
    `, ()=>{
      const from = $id('renameFrom').value;
      const to   = ($id('renameTo').value||'').trim();
      if(!to) return;
      headers = headers.map(h=> h===from ? to : h);
      data = data.map(row=>{ const o={}; Object.keys(row).forEach(k=>{ o[k===from?to:k]=row[k]; }); return o; });
      updateStats(); renderPreview(); dl.toolArea.innerHTML='';
      _applyToAppState(headers, data);
    });
  }

  function tool_filterRows(){
    const colOpts = headers.map(h=>`<option value="${esc(h)}">${esc(h)}</option>`).join('');
    buildToolCard('🔍 행 필터', `
      <div class="dl-group"><label>컬럼</label><select id="filterCol">${colOpts}</select></div>
      <div class="dl-group"><label>연산자</label>
        <select id="filterOp">
          <option value="contains">포함</option>
          <option value="eq">=</option>
          <option value="neq">≠</option>
          <option value="gt">&gt;</option>
          <option value="lt">&lt;</option>
          <option value="gte">≥</option>
          <option value="lte">≤</option>
        </select>
      </div>
      <div class="dl-group"><label>값</label><input id="filterVal" type="text" placeholder="비교 값"/></div>
    `, ()=>{
      const col = $id('filterCol').value;
      const op  = $id('filterOp').value;
      const valRaw = $id('filterVal').value;
      const numVal = parseFloat(valRaw);
      const isNum  = !isNaN(numVal);

      data = data.filter(row=>{
        const cell = row[col];
        const cellNum = parseFloat(cell);
        if(op==='contains') return String(cell).includes(valRaw);
        if(isNum && !isNaN(cellNum)){
          if(op==='eq')  return cellNum===numVal;
          if(op==='neq') return cellNum!==numVal;
          if(op==='gt')  return cellNum> numVal;
          if(op==='lt')  return cellNum< numVal;
          if(op==='gte') return cellNum>=numVal;
          if(op==='lte') return cellNum<=numVal;
        }else{
          if(op==='eq')  return String(cell)===valRaw;
          if(op==='neq') return String(cell)!==valRaw;
          if(op==='gt')  return String(cell)> valRaw;
          if(op==='lt')  return String(cell)< valRaw;
          if(op==='gte') return String(cell)>=valRaw;
          if(op==='lte') return String(cell)<=valRaw;
        }
        return true;
      });

      if(previewState.rowIdxs){
        previewState.rowIdxs = previewState.rowIdxs.filter(i => i>=0 && i<data.length);
      }
      updateStats(); renderPreview(); dl.toolArea.innerHTML='';
      _applyToAppState(headers, data);
    });
    // ▼ dataLab() IIFE의 가장 아래, 다른 버튼 바인딩들 끝난 다음에 추가
  };

  function tool_fillNa(){
    if(!headers.length){ alert('먼저 CSV를 불러와 주세요.'); return; }
    const isMissing = (v)=> v===undefined || v===null || v==='' || String(v).toLowerCase()==='nan';
    const total = data.length;
    const missing = headers.map(h=>{
      let cnt=0; for(const r of data){ if(isMissing(r[h])) cnt++; }
      return {col:h, cnt, pct: total ? (cnt/total*100) : 0};
    });
    const cand = missing.filter(m=>m.pct>0).map(m=>m.col);
    if(!cand.length){
      buildToolCard('🩹 결측치 채우기', `<div class="summary-item ok">채울 결측치가 없습니다.</div>`, ()=> dl.toolArea.innerHTML='');
      return;
    }
    const options = cand.map(h=>`<option value="${esc(h)}">${esc(h)}</option>`).join('');
    buildToolCard('🩹 결측치 채우기', `
      <div class="dl-group"><label>대상 컬럼</label><select id="fillNaCol">${options}</select></div>
      <div class="dl-group"><label>방법</label>
        <select id="fillNaStrategy">
          <option value="mean">mean(평균)</option>
          <option value="min">min(최소)</option>
          <option value="max">max(최대)</option>
          <option value="manual">직접입력</option>
          <option value="drop">제거(drop rows)</option>
        </select>
        <small id="fillNaHint" style="color:var(--text-secondary);display:block;margin-top:.25rem;">숫자 컬럼이 아니면 mean은 불가합니다.</small>
      </div>
      <div class="dl-group" id="fillNaManualRow" style="display:none;">
        <label>입력 값</label><input id="fillNaValue" type="text" placeholder="예: 0, N/A 등"/>
        <label style="display:flex;align-items:center;gap:.5rem;margin-top:.5rem;">
          <input id="fillNaCastNumber" type="checkbox"/> 숫자로 변환해서 채우기
        </label>
      </div>
    `, ()=>{
      const col = $id('fillNaCol').value;
      const strat = $id('fillNaStrategy').value;
      if(strat==='drop'){
        data = data.filter(r => !isMissing(r[col]));
        if(previewState.rowIdxs){
          previewState.rowIdxs = previewState.rowIdxs.filter(i => i>=0 && i<data.length);
        }
        updateStats(); renderPreview(); dl.toolArea.innerHTML=''; return;
      }
      const nonMiss = data.map(r=>r[col]).filter(v=>!isMissing(v));
      const nums = nonMiss.map(Number).filter(Number.isFinite);
      const mostlyNum = nonMiss.length>0 && (nums.length/nonMiss.length>=0.8);

      let fillVal;
      if(strat==='manual'){
        const valRaw = $id('fillNaValue').value;
        if($id('fillNaCastNumber').checked){
          const n = Number(valRaw);
          if(!Number.isFinite(n)){ alert('유효한 숫자를 입력하세요.'); return; }
          fillVal = n;
        } else fillVal = valRaw;
      } else if(strat==='mean'){
        if(!mostlyNum){ alert('숫자형이 아니라 mean 계산 불가. 직접입력을 사용하세요.'); return; }
        const s = nums.reduce((a,b)=>a+b,0); fillVal = nums.length ? s/nums.length : 0;
      } else if(strat==='min'){
        fillVal = mostlyNum ? (nums.length?Math.min(...nums):'')
                            : (nonMiss.length? nonMiss.slice().sort()[0] : '');
      } else if(strat==='max'){
        fillVal = mostlyNum ? (nums.length?Math.max(...nums):'')
                            : (nonMiss.length? nonMiss.slice().sort().reverse()[0] : '');
      }

      data = data.map(r => isMissing(r[col]) ? {...r, [col]:fillVal} : r);
      updateStats(); renderPreview(); dl.toolArea.innerHTML='';
      _applyToAppState(headers, data); 
    });

    const stratSel = $id('fillNaStrategy');
    const manualEl = $id('fillNaManualRow');
    stratSel.addEventListener('change', ()=>{ manualEl.style.display = stratSel.value==='manual' ? 'block':'none'; });
    manualEl.style.display = stratSel.value==='manual' ? 'block':'none';
  }

  function tool_sortBy(){
    const colOpts = headers.map(h=>`<option value="${esc(h)}">${esc(h)}</option>`).join('');
    buildToolCard('↕️ 정렬', `
      <div class="dl-group"><label>정렬 컬럼</label><select id="sortCol">${colOpts}</select></div>
      <div class="dl-group"><label>순서</label>
        <select id="sortDir">
          <option value="asc">오름차순</option>
          <option value="desc">내림차순</option>
        </select>
      </div>`, ()=>{
      const col = $id('sortCol').value;
      const dir = $id('sortDir').value;

      const isEmpty = v => v === undefined || v === null || v === '';
      const cmp = (a,b)=>{
        const ae=isEmpty(a), be=isEmpty(b);
        if(ae&&be) return 0; if(ae) return 1; if(be) return -1;
        const an=Number(a), bn=Number(b);
        if(Number.isFinite(an) && Number.isFinite(bn)) return an-bn;
        const at=Date.parse(a), bt=Date.parse(b);
        if(!isNaN(at) && !isNaN(bt)) return at-bt;
        return String(a).localeCompare(String(b),'ko',{numeric:true,sensitivity:'base'});
      };

      data.sort((r1,r2)=>{ const res=cmp(r1[col], r2[col]); return dir==='asc'?res:-res; });
      renderPreview(); dl.toolArea.innerHTML='';
      _applyToAppState(headers, data);
    });
  }

  // ====== [PATCH] 상세정보(파일 info) ======
  function tool_fileInfo(){
    if(!headers.length){ alert('먼저 CSV를 불러와 주세요.'); return; }

    // 중복 컬럼(원본 헤더 기준)
    const baseHdrs = (Array.isArray(rawHeaders) && rawHeaders.length) ? rawHeaders : headers;
    const counts = new Map();
    baseHdrs.forEach(h => counts.set(h, (counts.get(h)||0)+1));
    const duplicates = [...counts.entries()].filter(([,n])=>n>1).map(([h,n])=>`${h} ×${n}`);

    // 결측 요약
    const total = data.length;
    const isMissing = (v)=> v===undefined || v===null || v==='' || String(v).toLowerCase()==='nan';
    const missing = headers.map(h=>{
      let cnt=0; for(const r of data){ if(isMissing(r[h])) cnt++; }
      return { col:h, cnt, pct: total ? (cnt/total*100) : 0 };
    });
    const missingFiltered = missing.filter(m=>m.cnt>0 && m.pct>0);


    // 타입/고유값/표본(카테고리는 전체)
    function getUniqueValues(arr){
      const s=new Set(); for(const v of arr){ if(v!==undefined && v!==null && v!=='') s.add(String(v)); }
      return [...s];
    }
    const infoRows = headers.map(h=>{
      const type0 = inferColType_generic(data, h);
      const colVals = data.map(r=>r[h]);
      const uniquesArr = getUniqueValues(colVals);
      const uniques = uniquesArr.length;

      const sampleText = samplePrettyForColumn(colVals, type0);
      return { col:h, type:type0, uniques, sample:sampleText };
    });

    // 표 HTML
    const infoTable = `
      <table class="dl-table" style="margin-top:.5rem;">
        <thead><tr><th>컬럼</th><th>타입</th><th>고유값 수</th><th>예시/카테고리</th></tr></thead>
        <tbody>
          ${infoRows.map(r=>`<tr>
            <td>${esc(r.col)}</td>
            <td>${esc(r.type)}</td>
            <td>${r.uniques.toLocaleString()}</td>
            <td style="max-width:600px; overflow:auto; white-space:normal;">${esc(r.sample)}</td>
          </tr>`).join('')}
        </tbody>
      </table>`;

    const missTable = missingFiltered.length
      ? `<table class="dl-table" style="margin-top:.5rem;">
          <thead><tr><th>컬럼</th><th>결측치(개)</th><th>결측치(%)</th></tr></thead>
          <tbody>
            ${missingFiltered.map(m=>{
              let style='';
              if(m.pct>=90) style='style="background:rgba(239,68,68,0.23); color:#fecaca;"';
              else if(m.pct>=30) style='style="background:rgba(234,179,8,0.17); color:#fef9c3;"';
              else style='style="background:rgba(16,185,129,0.17); color:#86efac;"';
              return `<tr ${style}><td>${esc(m.col)}</td><td>${m.cnt.toLocaleString()}</td><td>${m.pct.toFixed(2)}%</td></tr>`;
            }).join('')}
          </tbody>
        </table>`
      : `<div class="summary-item ok" style="margin-top:.5rem;">결측치 없음</div>`;

    // 대략 메모리
    let approxBytes=0; try{ approxBytes = new Blob([JSON.stringify(data)]).size; }catch(e){ approxBytes = JSON.stringify(data).length; }
    const fmt = (n)=> n>1<<30 ? (n/(1<<30)).toFixed(2)+' GB' : n>1<<20 ? (n/(1<<20)).toFixed(2)+' MB' : n>1<<10 ? (n/(1<<10)).toFixed(2)+' KB' : n+' B';

    const headerInfo = `
      <div class="summary-item" style="background:rgba(37,99,235,0.1);color:#93c5fd;">
        총 ${total.toLocaleString()} rows × ${headers.length.toLocaleString()} cols · 약 ${fmt(approxBytes)}
      </div>`;

    const dupBlock = duplicates.length
      ? `<div><strong>중복 컬럼</strong>
          <div class="summary-item duplicate" style="margin-top:.5rem;">${esc(duplicates.join(', '))}</div>
        </div>`
      : `<div><strong>중복 컬럼</strong>
          <div class="summary-item ok" style="margin-top:.5rem;">중복 컬럼 없음</div>
        </div>`;

    // 툴 카드로 출력
    buildToolCard('ℹ️ 파일 상세정보', `
      <div class="tool-card-inner" style="display:grid; gap:.75rem; width:100%;">
        ${headerInfo}
        ${dupBlock}
        <div><strong>결측치 요약</strong>${missTable}</div>
        <div><strong>기본 정보 (pd.info 유사)</strong>${infoTable}</div>
      </div>
    `, ()=>{ dl.toolArea.innerHTML=''; });
  }

  // ================== [ADD] 이상치 유틸 ==================
function _isFiniteNumber(v){ const n = parseFloat(String(v).trim()); return Number.isFinite(n); }
function _colValuesAsNumbers(data, col){ return data.map(r => parseFloat(String(r[col]).trim())).filter(Number.isFinite); }
function iqrBounds(nums){
  if(!nums.length) return {low: null, high: null};
  const a = nums.slice().sort((x,y)=>x-y);
  const q = p => {
    const pos = (a.length-1)*p, lo=Math.floor(pos), hi=Math.ceil(pos);
    return lo===hi ? a[lo] : a[lo] + (a[hi]-a[lo])*(pos-lo);
  };
  const Q1 = q(0.25), Q3 = q(0.75), IQR = Q3 - Q1;
  return { low: Q1 - 1.5*IQR, high: Q3 + 1.5*IQR };
}
function zscoreMask(nums, thresh){
  const n = nums.length; if(!n) return nums.map(_=>false);
  const mean = nums.reduce((s,x)=>s+x,0)/n;
  const std = Math.sqrt(nums.reduce((s,x)=>s+(x-mean)**2,0)/n) || 0;
  return nums.map(x => std===0 ? false : Math.abs((x-mean)/std) > thresh);
}

  // ================== [ADD] 이상치 처리 툴 ==================
  function renderOutlierTool(dl, headers, data, rerenderPreview){
    const numericCols = headers.filter(h => _colValuesAsNumbers(data, h).length > 0);

    dl.toolArea.innerHTML = `
      <div class="tool-card">
        <h3>📈 이상치 처리</h3>
        <div class="tool-inline">
          <div class="dl-group">
            <label>컬럼 선택</label>
            <select id="ot-col" ${numericCols.length? '' : 'disabled'}>
              ${numericCols.map(c=>`<option value="${c}">${c}</option>`).join('') || '<option>(숫자 컬럼 없음)</option>'}
            </select>
          </div>
          <div class="dl-group">
            <label>방법</label>
            <select id="ot-method">
              <option value="iqr">IQR (1.5×IQR)</option>
              <option value="z">Z-score</option>
            </select>
          </div>
          <div class="dl-group" id="ot-z-wrap" style="display:none;">
            <label>Z 임계값</label>
            <input id="ot-z" type="number" step="0.1" value="3">
          </div>
          <div class="dl-group">
            <label>액션</label>
            <select id="ot-action">
              <option value="mark">표시만 (하이라이트)</option>
              <option value="drop">행 제거</option>
              <option value="cap">경계로 캡핑 (윈저라이즈)</option>
              <option value="nan">결측 처리</option>
            </select>
          </div>
        </div>
        <div class="tool-actions">
          <button class="chart-btn" id="ot-preview">미리보기 반영</button>
          <button class="chart-btn" id="ot-apply">데이터에 적용</button>
        </div>
        <div class="dl-stats" id="ot-info"></div>
      </div>
    `;
    // === [정렬/높이 맞춤 — 이 카드에만 적용] ===
    const _wrap = dl.toolArea.querySelector('.tool-inline');
    if (_wrap) _wrap.style.alignItems = 'flex-end';  // 하단 기준선 맞추기
    ['ot-col','ot-method','ot-action','ot-z'].forEach(id=>{
      const el = document.getElementById(id);
      if (el) el.style.height = '36px';              // 셀렉트/인풋 높이 통일
    });

    const $ = id => document.getElementById(id);
    const selCol = $('ot-col'), selMethod = $('ot-method'), zWrap=$('ot-z-wrap'), zInput=$('ot-z');
    const info = $('ot-info');

    selMethod.addEventListener('change', ()=>{
      zWrap.style.display = (selMethod.value==='z') ? 'block' : 'none';
    });

    function computeMask(col){
      const numsFull = data.map(r => _isFiniteNumber(r[col]) ? parseFloat(r[col]) : null);
      const valid = numsFull.filter(v => v!==null);
      if(selMethod.value === 'iqr'){
        const {low, high} = iqrBounds(valid);
        const mask = numsFull.map(v => v===null ? false : (v<low || v>high));
        return {mask, low, high, meta:`IQR 경계 [${low?.toFixed(4)}, ${high?.toFixed(4)}]`};
      }else{
        const thr = Math.abs(parseFloat(zInput.value)) || 3;
        const idxMap = new Map(); const arr = [];
        numsFull.forEach((v,i)=>{ if(v!==null){ idxMap.set(i, arr.length); arr.push(v); } });
        const zmask = zscoreMask(arr, thr);
        const mask = numsFull.map((v,i) => v===null ? false : !!zmask[idxMap.get(i)]);

        return {mask, low:null, high:null, meta:`Z-score 임계값 ${thr}`};
      }
    }

    let lastMarked = null;
    $('ot-preview').addEventListener('click', ()=>{
      if(!numericCols.length){ info.textContent = '숫자 컬럼이 없습니다.'; return; }
      const col = selCol.value;
      const {mask, low, high, meta} = computeMask(col);
      const idxs = new Set(); mask.forEach((isOut,i)=>{ if(isOut) idxs.add(i); });
      lastMarked = { col, indices: idxs, meta, low, high };
      info.textContent = `표시할 이상치: ${idxs.size.toLocaleString()}개 · ${meta}`;
      rerenderPreview(lastMarked);
    });

    $('ot-apply').addEventListener('click', ()=>{
      if(!numericCols.length){ info.textContent = '숫자 컬럼이 없습니다.'; return; }
      const col = selCol.value;
      const {mask, low, high, meta} = computeMask(col);
      const action = $('ot-action').value;

      let affected = 0;
      if(action === 'drop'){
        const newData = [];
        data.forEach((row, i)=>{ if(mask[i]) affected++; else newData.push(row); });
        data.length = 0; data.push(...newData);
      }else if(action === 'cap'){
        let L = low, U = high;
        if(L==null || U==null){
          const nums = _colValuesAsNumbers(data, col);
          const b = iqrBounds(nums); L = b.low; U = b.high;
        }
        data.forEach((row,i)=>{
          if(!_isFiniteNumber(row[col])) return;
          let v = parseFloat(row[col]);
          const nv = Math.min(Math.max(v, L), U);
          if(nv !== v && mask[i]){ row[col] = String(nv); affected++; }
        });
      }else if(action === 'nan'){
        data.forEach((row,i)=>{ if(mask[i]){ row[col] = ''; affected++; } });
      }else{
        const idxs = new Set(); mask.forEach((m,i)=>{ if(m) idxs.add(i); });
        lastMarked = { col, indices: idxs, meta, low, high };
        rerenderPreview(lastMarked);
        info.textContent = `표시된 이상치: ${idxs.size.toLocaleString()}개 · ${meta}`;
        return;
      }

      info.textContent = `적용 완료: ${affected.toLocaleString()}개 · ${meta}`;
      rerenderPreview(null);
    });
  }


  // 버튼 바인딩
  dl.tools.forEach(btn=>{
    btn.addEventListener('click', ()=>{
      const tool = btn.dataset.tool;
      // if(tool!=='fileInfo' && !headers.length){ alert('먼저 CSV를 불러와 주세요.'); return; }
      if(tool==='dropCols')   return tool_dropCols();
      if(tool==='renameCol')  return tool_renameCol();
      if(tool==='filterRows') return tool_filterRows();
      if(tool==='fillNa')     return tool_fillNa();
      if(tool==='sortBy')     return tool_sortBy();
      if(tool==='fileInfo')   return tool_fileInfo();

      // ✨ 추가
      if(tool==='outliers') {
        return renderOutlierTool(dl, headers, data, (markInfo)=>renderPreview(markInfo));
      }
    });
  });

  function hasHeaderChangedAgainstDBSchema(dbCols){
  // dbCols: 백엔드에서 /tables/:name/columns 같은 엔드포인트로 받아온 순서/이름
  if (dbCols.length !== headers.length) return true;
  for(let i=0;i<dbCols.length;i++){
    if (dbCols[i] !== headers[i]) return true;
  }
  return false;
}
// 덮어쓰기 클릭 시, 불일치면 alert 띄우고 중단하거나 save_as로 유도


  // 미리보기 적용(샘플 확정)
  dl.applyPrev?.addEventListener('click', applyPreview);

  // 가공본 다운로드
  dl.downloadBtn?.addEventListener('click', ()=>{
    if(!headers.length) return;

    // 원본 헤더 대응표 (key=현재 키, raw=원본 헤더)
    const pairs = headers.map((h,i)=>({
      key: h,
      raw: (Array.isArray(rawHeaders) && rawHeaders[i]) ? rawHeaders[i] : h
    }));


    // DB에서 온 데이터라도, 사용자가 rename 했다면 현재 headers를 우선
    const useCurrentHeaders = true;  // 편집본 우선
    const headerLine = (useCurrentHeaders ? headers : pairs.map(p=>p.raw))
      .map(h => `"${String(h).replace(/"/g,'""')}"`)
      .join(',');


    // 2) 데이터 라인: key 순서대로 값 추출
    let csv = headerLine + '\n';
    for(const row of data){
      const line = pairs.map(p=>{
        const v = row[p.key] ?? '';
        return `"${String(v).replace(/"/g,'""')}"`;
      }).join(',');
      csv += line + '\n';
    }

    const blob = new Blob([csv], {type:'text/csv;charset=utf-8;'});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a'); a.href = url; a.download = 'processed.csv';
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    URL.revokeObjectURL(url);
  });

})();

// ==== 학습 모달 & 호출 ====
(function trainingUI(){
  const btnOpen  = document.getElementById('ts-btnTrain');
  const modal    = document.getElementById('ts-trainModal');
  const selTargets = document.getElementById('trainTargets');
  const btnRun = document.getElementById('trainRun');
  const btnCancel = document.getElementById('trainCancel');
  const logEl = document.getElementById('trainLog');
  const horizonEl = document.getElementById('trainHorizon');
  const useLLMEl = document.getElementById('trainUseLLM');

  function openModal(){
    // 타깃 목록 채우기
    selTargets.innerHTML = '';
    const headers = (window.AppState?.headers || []);
    headers.forEach(h=>{
      const opt = document.createElement('option');
      opt.value = h; opt.textContent = h;
      selTargets.appendChild(opt);
    });
    logEl.textContent = '';
    modal.style.display = 'flex';
  }

  function closeModal(){ modal.style.display = 'none'; }

  async function runTrain(){
    const targets = Array.from(selTargets?.options || [])
      .filter(o => o.selected)
      .map(o => o.value);
    if(targets.length===0){
      alert('타깃 컬럼을 1개 이상 선택하세요.');
      return;
    }
    // 현재 그리드 데이터 → 배열
    const { headers, rows } = (typeof gridToArrays === 'function')
    ? gridToArrays()
    : (() => {
        const H = (window.AppState.headers || []);
        const R = (window.AppState.dataset || []).map(r => H.map(h => r[h]));
        return { headers: H, rows: R };
      })();
    
    const body = {
      headers,
      rows,
      targets,
      horizon: parseInt(horizonEl.value)||14,
      use_llm: !!useLLMEl.checked,
      table_name: window.AppState?.name || null,
    };

    logEl.textContent = '학습 요청 중...';
    try{
      const res = await fetch(`${BACKEND_BASE_URL}/auto_train`, {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify(body)
      });
      const js = await res.json();
      if(!res.ok) throw js;

      // 결과 표시
      logEl.textContent = JSON.stringify(js, null, 2);

      // (선택) 예측 스낵토스트
      alert('학습 완료! 요약:\n' + (js?.summary || 'done'));
    }catch(e){
      logEl.textContent = '오류: ' + (e?.detail || JSON.stringify(e));
    }
  }

  btnOpen?.addEventListener('click', openModal);
  btnCancel?.addEventListener('click', closeModal);
  btnRun?.addEventListener('click', runTrain);

  // 오버레이 클릭 닫기
  // modal?.addEventListener('click', (e)=>{ if(e.target===modal) closeModal(); });
})();

// ================== 수요예측 학습 모달 ==================
(function forecastTrainModal(){
  const btnOpen  = document.getElementById('btnTrain');         // 수요예측 화면의 "학습" 버튼
  const modal    = document.getElementById('trainModal');       // 우리가 index.html에 방금 만든 모달
  const selTable = document.getElementById('ts-table-select');
  const selCols  = document.getElementById('ts-target-cols');
  const btnClose = document.getElementById('ts-train-close');
  const btnRun   = document.getElementById('ts-train-run');

  if (!btnOpen || !modal) return;

  // 1) 모달 열기
  btnOpen.addEventListener('click', async () => {
    modal.style.display = 'flex';
    // 열릴 때마다 시계열 테이블만 다시 불러옴
    await loadTimeseriesTables();
    // 컬럼 비우기
    if (selCols) selCols.innerHTML = '';
  });

  // 2) 닫기
  btnClose?.addEventListener('click', () => {
    modal.style.display = 'none';
  });

  // 3) 테이블 바뀌면 그 테이블 컬럼 가져오기
  selTable?.addEventListener('change', async (e) => {
    const tbl = e.target.value;
    await loadColumnsForTable(tbl);
  });

  // 4) 학습 시작 버튼
  btnRun?.addEventListener('click', async () => {
    const tbl = selTable.value;
    const cols = Array.from(selCols.selectedOptions || []).map(o => o.value);
    const horizon = Number(document.getElementById('ts-horizon')?.value || 14);

    if (!tbl) {
      alert('시계열 테이블을 먼저 선택하세요.');
      return;
    }
    if (!cols.length) {
      alert('타깃 컬럼을 1개 이상 선택하세요.');
      return;
    }

    console.log('학습 시작:', { table_name: tbl, target_cols: cols, horizon });

    try {
      const res = await fetch(`${BACKEND_BASE_URL}/train_from_table`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          table_name: tbl,
          target_cols: cols,
          horizon: horizon
        })
      });
      const js = await res.json();
      if (!res.ok) throw js;
      alert('학습 완료: ' + (js.summary || 'done'));

      if (js.forecast) {
        renderForecastChart(js.forecast);
      }
    } catch (err) {
      console.error(err);
      alert('학습 실패: ' + (err?.detail || JSON.stringify(err)));
    } finally {
      modal.style.display = 'none';
    }
  });


  // ---------------- helper들 ----------------
  async function loadTimeseriesTables(){
    if (!selTable) return;
    selTable.innerHTML = '<option value="">(불러오는 중...)</option>';
    try {
      const js = await fetchJSON('GET', `${BACKEND_BASE_URL}/timeseries-tables`);
      selTable.innerHTML = '<option value="">(시계열 테이블 선택)</option>';
      (js.tables || []).forEach(t => {
        const opt = document.createElement('option');
        opt.value = t;
        opt.textContent = t;
        selTable.appendChild(opt);
      });
    } catch (err) {
      selTable.innerHTML = '<option value="">(불러오기 실패)</option>';
    }
  }

  async function loadColumnsForTable(tableName){
    if (!selCols) return;
    selCols.innerHTML = '';
    if (!tableName) return;
    try {
      const js = await fetchJSON('POST', `${BACKEND_BASE_URL}/table-columns`, { table_name: tableName });
      (js.columns || []).forEach(c => {
        const opt = document.createElement('option');
        opt.value = c;
        opt.textContent = c;
        selCols.appendChild(opt);
      });
    } catch (err) {
      const opt = document.createElement('option');
      opt.textContent = '(컬럼 불러오기 실패)';
      selCols.appendChild(opt);
    }
  }

  async function loadAllTables() {
    const selTable = document.getElementById('ts-table-select');
    if (!selTable) return;

    // 전체 테이블 가져오기
    const res = await fetch(`${BACKEND_BASE_URL}/tables`);
    const js = await res.json();

    selTable.innerHTML = '<option value="">(테이블 선택)</option>';
    (js.tables || []).forEach(t => {
      const opt = document.createElement('option');
      opt.value = t;
      opt.textContent = t;
      selTable.appendChild(opt);
    });
  }
})();


// ✅ 미리보기 표: 마우스 왼쪽 버튼 드래그로만 좌우 스크롤
(function enablePreviewDragScroll(){
  const scroller = document.getElementById('previewTableWrapper');
  if(!scroller) return;

  let isDown = false;
  let startX = 0;
  let startLeft = 0;

  scroller.addEventListener('mousedown', (e)=>{
    // 왼쪽 버튼만 (e.button === 0)
    if (e.button !== 0) return;

    // UI 요소 위에서 시작하면 패스(원하면 유지/삭제)
    if (e.target.closest('a,button,input,select,textarea,label')) return;

    isDown = true;
    scroller.classList.add('dragging');
    startX = e.pageX;
    startLeft = scroller.scrollLeft;

    // 텍스트 선택 방지 + 드래그 제스처 고정
    e.preventDefault();
  });

  window.addEventListener('mousemove', (e)=>{
    if(!isDown) return;
    // 마우스가 눌린 상태인지(브라우저 포커스 변화 대비)
    if ((e.buttons & 1) !== 1) { // 왼쪽 버튼이 아니면 종료
      isDown = false;
      scroller.classList.remove('dragging');
      return;
    }
    const dx = e.pageX - startX;
    scroller.scrollLeft = startLeft - dx;
  });

  const endDrag = ()=>{
    if(!isDown) return;
    isDown = false;
    scroller.classList.remove('dragging');
  };

  window.addEventListener('mouseleave', endDrag);
  window.addEventListener('mouseup', endDrag);
  window.addEventListener('blur', endDrag);

  // ❌ 휠 핸들러 없음: 기본 동작 유지(페이지 상하 스크롤 그대로)
})();

function renderInventorySummary(data) {
  const tableBody = document.querySelector('#inv-summary-table tbody');
  if (!tableBody) return;

  const { rows, total, warehouse_col, qty_col } = data;
  const maxQty = Math.max(...rows.map(r => r.qty));
  let htmlRows = '';

  rows.forEach(r => {
    const ratio = (r.qty / maxQty) * 100;
    htmlRows += `
      <tr>
        <td>${esc(r.warehouse)}</td>
        <td>
          <div class="bar-wrap">
            <div class="bar" style="width:${ratio.toFixed(1)}%"></div>
          </div>
        </td>
        <td>${r.qty.toLocaleString()}</td>
      </tr>`;
  });

  tableBody.innerHTML = htmlRows;
  document.getElementById('inv-summary').textContent =
    `총합: ${total.toLocaleString()} (${rows.length}개 창고)`;
}


// === [INV] 서버 호출 ===
async function runInventorySummary(){
  const BASE = () => BACKEND_BASE_URL;
  const t = document.getElementById('inv-tableSelect')?.value?.trim();
  const w = document.getElementById('inv-warehouseColSel')?.value?.trim();
  const q = document.getElementById('inv-qtyColSel')?.value?.trim();

  if(!t) return alert('테이블을 선택하세요.');
  if(!w) return alert('창고 컬럼명을 입력하세요.');
  if(!q) return alert('재고 컬럼명을 입력하세요.');

  try{
    const res = await fetch(`${BASE()}/inventory/summary`, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ table_name: t, warehouse_col: w, qty_col: q })
    });
    const js = await res.json();
    if(!res.ok) throw new Error(js.detail || 'inventory/summary failed');
    renderInventorySummary(js);
  }catch(err){
    alert('요약 실패: ' + err.message);
  }
}

document.getElementById('inv-run')?.addEventListener('click', runInventorySummary);

// ✅ 테이블 바뀔 때 컬럼 불러오기
document.getElementById('inv-tableSelect')?.addEventListener('change', (e)=>{
  loadInventoryColumns(e.target.value);
});

// ===== 수요예측 캔버스 렌더 =====
function renderForecastChart(fc) {
  const el = document.getElementById('forecastChart');
  if (!el || !fc) return;

  // 1) 캔버스 해상도 올리기 (흐릿함 제거)
  const dpr = window.devicePixelRatio || 1;
  const cssWidth = el.clientWidth || 1024;
  const cssHeight = el.clientHeight || 280;
  el.width = cssWidth * dpr;
  el.height = cssHeight * dpr;

  const ctx = el.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0); // 좌표계를 CSS 기준으로
  ctx.clearRect(0, 0, cssWidth, cssHeight);

  // 2) 데이터 정리
  const history = Array.isArray(fc.history) ? fc.history : [];
  const future  = Array.isArray(fc.future)  ? fc.future  : [];

  // 너무 많으면 샘플링 (성능+가독성)
  const MAX_POINTS = 180;    // 화면에 최대 180점만
  function downsample(arr, max) {
    if (arr.length <= max) return arr;
    const step = arr.length / max;
    const out = [];
    for (let i = 0; i < arr.length; i += step) {
      out.push(arr[Math.floor(i)]);
    }
    return out;
  }
  const hData = downsample(history, MAX_POINTS);
  const fData = downsample(future,  MAX_POINTS / 3);

  const all = hData.concat(fData);
  if (!all.length) return;

  // 숫자만 뽑아서 min/max
  const values = all
    .map(p => typeof p.value === 'number' ? p.value : Number(p.value))
    .filter(v => Number.isFinite(v));

  const minV = Math.min(...values);
  const maxV = Math.max(...values);

  // 여백
  const paddingLeft = 50;
  const paddingRight = 140;   // 오른쪽에 라벨 찍을 공간
  const paddingTop = 20;
  const paddingBottom = 30;

  const plotW = cssWidth - paddingLeft - paddingRight;
  const plotH = cssHeight - paddingTop - paddingBottom;

  // 값→y 변환
  const toY = (v) => {
    if (!Number.isFinite(v)) return paddingTop + plotH;
    if (maxV === minV) return paddingTop + plotH / 2;
    const ratio = (v - minV) / (maxV - minV);
    return paddingTop + (1 - ratio) * plotH;
  };
  // 인덱스→x 변환
  const totalPts = hData.length + fData.length;
  const toX = (idx) => {
    if (totalPts <= 1) return paddingLeft;
    return paddingLeft + (idx / (totalPts - 1)) * plotW;
  };

  // 3) 배경 그리드
  ctx.strokeStyle = 'rgba(15,23,42,0.05)';
  ctx.lineWidth = 1;
  const GRID_ROWS = 5;
  for (let i = 0; i <= GRID_ROWS; i++) {
    const y = paddingTop + (plotH / GRID_ROWS) * i;
    ctx.beginPath();
    ctx.moveTo(paddingLeft, y);
    ctx.lineTo(paddingLeft + plotW, y);
    ctx.stroke();

    // y축 숫자
    const val = maxV - (maxV - minV) * (i / GRID_ROWS);
    ctx.fillStyle = 'rgba(15,23,42,0.45)';
    ctx.font = '12px sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText(formatNumber(val), paddingLeft - 6, y + 4);
  }

  // 4) 과거 라인 (파란색)
  ctx.beginPath();
  ctx.strokeStyle = '#4f46e5';
  ctx.lineWidth = 2;
  hData.forEach((p, i) => {
    const x = toX(i);
    const y = toY(p.value);
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();

  // 5) 예측 라인 (주황 점선)
  if (fData.length) {
    ctx.save();
    ctx.setLineDash([6, 4]);
    ctx.beginPath();
    ctx.strokeStyle = '#f97316';
    ctx.lineWidth = 2;
    fData.forEach((p, i) => {
      const idx = hData.length + i;
      const x = toX(idx);
      const y = toY(p.value);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
    ctx.restore();
  }

  // 6) x축 날짜 라벨 4~6개 정도만
  const showDates = hData; // 과거 기준으로 찍자
  const LABEL_COUNT = Math.min(6, showDates.length);
  if (LABEL_COUNT > 0) {
    ctx.fillStyle = 'rgba(15,23,42,0.55)';
    ctx.font = '11px sans-serif';
    ctx.textAlign = 'center';
    for (let i = 0; i < LABEL_COUNT; i++) {
      const idx = Math.floor((showDates.length - 1) * (i / (LABEL_COUNT - 1 || 1)));
      const p = showDates[idx];
      const x = toX(idx);
      const label = p.date ? p.date.slice(5) : String(idx);
      ctx.fillText(label, x, paddingTop + plotH + 16);
    }
  }

  // 7) 오른쪽 요약 박스
  const lastHist = hData[hData.length - 1];
  const lastPred = fData[fData.length - 1];
  const boxX = paddingLeft + plotW + 12;
  const boxY = paddingTop + 8;
  const boxW = 120;
  const boxH = 70;

  ctx.fillStyle = 'rgba(148,163,184,0.12)';
  ctx.strokeStyle = 'rgba(148,163,184,0.35)';
  ctx.lineWidth = 1;
  roundRect(ctx, boxX, boxY, boxW, boxH, 10, true, true);

  ctx.fillStyle = '#0f172a';
  ctx.font = '11px sans-serif';
  ctx.fillText('예측 개요', boxX + 10, boxY + 16);

  ctx.font = '11px sans-serif';
  ctx.fillStyle = '#4f46e5';
  ctx.fillText(`최근: ${lastHist ? formatNumber(lastHist.value) : '-'}`, boxX + 10, boxY + 34);
  ctx.fillStyle = '#f97316';
  ctx.fillText(`예측: ${lastPred ? formatNumber(lastPred.value) : '-'}`, boxX + 10, boxY + 52);

  // 8) 범례
  ctx.fillStyle = '#4f46e5';
  ctx.fillRect(paddingLeft, paddingTop - 14, 18, 4);
  ctx.fillStyle = '#0f172a';
  ctx.font = '11px sans-serif';
  ctx.fillText('실제/과거', paddingLeft + 24, paddingTop - 10);

  ctx.strokeStyle = '#f97316';
  ctx.setLineDash([6, 4]);
  ctx.beginPath();
  ctx.moveTo(paddingLeft + 90, paddingTop - 12);
  ctx.lineTo(paddingLeft + 108, paddingTop - 12);
  ctx.stroke();
  ctx.setLineDash([]);
  ctx.fillStyle = '#0f172a';
  ctx.fillText('예측', paddingLeft + 116, paddingTop - 10);

  // ---- 내부 유틸 ----
  function formatNumber(v) {
    if (!Number.isFinite(v)) return '-';
    if (v >= 1000000) return (v / 1000000).toFixed(1) + 'M';
    if (v >= 1000) return Math.round(v).toLocaleString();
    return Number(v).toFixed(0);
  }
  function roundRect(ctx, x, y, w, h, r, fill, stroke) {
    if (typeof r === 'number') {
      r = {tl: r, tr: r, br: r, bl: r};
    } else {
      r = {tl: r.tl || 0, tr: r.tr || 0, br: r.br || 0, bl: r.bl || 0};
    }
    ctx.beginPath();
    ctx.moveTo(x + r.tl, y);
    ctx.lineTo(x + w - r.tr, y);
    ctx.quadraticCurveTo(x + w, y, x + w, y + r.tr);
    ctx.lineTo(x + w, y + h - r.br);
    ctx.quadraticCurveTo(x + w, y + h, x + w - r.br, y + h);
    ctx.lineTo(x + r.bl, y + h);
    ctx.quadraticCurveTo(x, y + h, x, y + h - r.bl);
    ctx.lineTo(x, y + r.tl);
    ctx.quadraticCurveTo(x, y, x + r.tl, y);
    ctx.closePath();
    if (fill) ctx.fill();
    if (stroke) ctx.stroke();
  }
}

// ✅ 수요예측 전역 상태
window.ForecastState = {
  meta: null,     // 백엔드에서 받은 전체 응답
  series: {},     // 제품코드 -> {history, future}
};

// =============== 수요예측 UI ===============
(function(){
  const btnTrain = $id('btnTrain');
  const modal = $id('trainModal');
  const selTable = $id('train-table-select');
  const selTargets = $id('train-target-cols');
  const selTime = $id('train-time-col');
  const selProdCol = $id('train-product-col');
  const inputHorizon = $id('train-horizon');
  const btnStart = $id('train-start');
  const btnClose = $id('train-close');
  const productSelect = $id('forecast-product-select');
  const chartCanvas = $id('forecastChart');

  // 모달 열기
  btnTrain?.addEventListener('click', async ()=>{
    modal.style.display = 'flex';
    // 테이블 목록
    const t = await fetchJSON('GET', `${BACKEND_BASE_URL}/tables`);
    selTable.innerHTML = '<option value="">(테이블 선택)</option>';
    (t.tables || []).forEach(name=>{
      const opt = document.createElement('option');
      opt.value = name;
      opt.textContent = name;
      selTable.appendChild(opt);
    });
  });

  // 모달 닫기
  btnClose?.addEventListener('click', ()=> modal.style.display = 'none');

  // 테이블 선택하면 컬럼 조회해서 3군데에 뿌리기
  selTable?.addEventListener('change', async (e)=>{
    const tableName = e.target.value;
    if (!tableName) return;
    const colsRes = await fetchJSON('POST', `${BACKEND_BASE_URL}/table-columns`, { table_name: tableName });
    const cols = colsRes.columns || [];

    // 타깃
    selTargets.innerHTML = '';
    cols.forEach(c=>{
      const opt = document.createElement('option');
      opt.value = c; opt.textContent = c;
      selTargets.appendChild(opt);
    });

    // 기준일
    selTime.innerHTML = '<option value="">(날짜/일자 컬럼 선택)</option>';
    cols.forEach(c=>{
      const opt = document.createElement('option');
      opt.value = c; opt.textContent = c;
      const lc = c.toLowerCase();
      if (lc.includes('date') || lc.includes('day') || lc.includes('일자') || lc.includes('기준일')) {
        selTime.prepend(opt);
      } else {
        selTime.appendChild(opt);
      }
    });

    // 제품코드
    selProdCol.innerHTML = '<option value="">(제품/품번 컬럼 선택)</option>';
    cols.forEach(c=>{
      const opt = document.createElement('option');
      opt.value = c; opt.textContent = c;
      const lc = c.toLowerCase();
      if (lc.includes('product') || lc.includes('item') || lc.includes('sku') || lc.includes('code') || lc.includes('품번') || lc.includes('품목')) {
        selProdCol.prepend(opt);
      } else {
        selProdCol.appendChild(opt);
      }
    });
  });

  // 학습 시작
  btnStart?.addEventListener('click', async ()=>{
    const table_name = selTable.value;
    const target_cols = Array.from(selTargets.selectedOptions).map(o=>o.value);
    const time_col = selTime.value;
    const product_col = selProdCol.value;
    const horizon = Number(inputHorizon.value || 14);

    if (!table_name) return alert('테이블을 선택하세요.');
    if (!target_cols.length) return alert('타깃 컬럼을 최소 1개 선택하세요.');
    if (!time_col) return alert('기준일 컬럼을 선택하세요.');
    if (!product_col) return alert('제품코드 컬럼을 선택하세요.');

    try {
      const res = await fetchJSON('POST', `${BACKEND_BASE_URL}/train_from_table`, {
        table_name,
        target_cols,
        time_col,
        product_col,
        horizon,
      });

      // 전역에 저장
      window.ForecastState.meta = res;
      window.ForecastState.series = res.series || {};

      // 제품코드 셀렉트 채우기
      productSelect.innerHTML = '<option value="">(제품코드 선택)</option>';
      (res.products || []).forEach(code=>{
        const opt = document.createElement('option');
        opt.value = code;
        opt.textContent = code;
        productSelect.appendChild(opt);
      });
      productSelect.style.display = 'block';

      // 모달 닫기
      modal.style.display = 'none';

      // 첫번째 제품으로 바로 그려주기
      if ((res.products || []).length > 0) {
        const first = res.products[0];
        productSelect.value = first;
        renderOneProduct(chartCanvas, res.series[first], res.time_col, res.target);
      } else {
        alert('제품코드가 유효하지 않습니다.');
      }
    } catch (err) {
      console.error(err);
      alert('학습 실패: ' + JSON.stringify(err));
    }
  });

  // 제품코드 바꿀 때마다 차트 다시 그림
  productSelect?.addEventListener('change', (e)=>{
    const code = e.target.value;
    const all = window.ForecastState.series || {};
    const data = all[code];
    if (!data) return;
    renderOneProduct(chartCanvas, data, window.ForecastState.meta.time_col, window.ForecastState.meta.target);
  });

})();

let forecastChartInstance = null;

function renderOneProduct(canvas, productData, timeColName, targetName){
  if (!canvas || !productData) return;

  const labels = [];
  const actual = [];
  const future = [];

  (productData.history || []).forEach(p=>{
    labels.push(p.date);
    actual.push(p.value ?? null);
    future.push(null);
  });
  (productData.future || []).forEach(p=>{
    labels.push(p.date);
    actual.push(null);
    future.push(p.value ?? null);
  });

  const ctx = canvas.getContext('2d');
  if (forecastChartInstance) {
    forecastChartInstance.destroy();
  }
  forecastChartInstance = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: '실제/과거',
          data: actual,
          borderColor: '#4f46e5',
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.25,
        },
        {
          label: '예측',
          data: future,
          borderColor: '#f97316',
          borderWidth: 2,
          borderDash: [6,4],
          pointRadius: 0,
          tension: 0.25,
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: true, position: 'top' },
        title: {
          display: true,
          text: `${productData.product_code} · ${targetName || ''}`
        }
      },
      scales: {
        x: { grid: { display: false }, ticks: { maxTicksLimit: 10 } },
        y: { beginAtZero: true, grid: { color: 'rgba(15,23,42,0.05)' } }
      }
    }
  });
}



// ===== 재고 모달 유틸 =====
function showInvModal() {
  const modal = document.getElementById('invModal');
  if (!modal) return;
  modal.style.display = 'flex';
}

function hideInvModal() {
  const modal = document.getElementById('invModal');
  if (!modal) return;
  modal.style.display = 'none';
}

async function fetchTables() {
  // 백엔드: GET /tables → { tables: ["t1","t2", ...] } 형태라고 가정
  const res = await fetch(`${BACKEND_BASE_URL}/tables`);
  const js = await res.json();
  if (!res.ok) throw new Error(js.detail || 'failed to load tables');
  // 응답 형태에 맞춰 조정
  return js.tables || js || [];
}

async function fetchTableColumns(tableName) {
  // 0) 최우선: POST /table-columns
  try {
    const js = await fetchJSON('POST', `${BACKEND_BASE_URL}/table-columns`, { table_name: tableName });
    if (Array.isArray(js.columns)) return js.columns;
  } catch (_) {}

  // 1) 차선: GET /table/columns
  try {
    const res = await fetch(
      `${BACKEND_BASE_URL}/table/columns?table_name=${encodeURIComponent(tableName)}`
    );
    const js = await res.json();
    if (res.ok && Array.isArray(js.columns)) return js.columns;
  } catch (_) {}

  // 2) 최후: /table/preview?limit=1 에서 첫 행 키 추출
  try {
    const res = await fetch(
      `${BACKEND_BASE_URL}/table/${encodeURIComponent(tableName)}/preview?limit=5000`
    );

    const js = await res.json();
    if (res.ok && Array.isArray(js.rows) && js.rows.length > 0) {
      return Object.keys(js.rows[0]);
    }
  } catch (_) {}

  throw new Error('columns not available');
}


function fillSelect(selectEl, items, placeholder) {
  selectEl.innerHTML = '';
  if (placeholder) {
    const opt0 = document.createElement('option');
    opt0.value = '';
    opt0.textContent = placeholder;
    selectEl.appendChild(opt0);
  }
  items.forEach(col => {
    const opt = document.createElement('option');
    opt.value = col;
    opt.textContent = col;
    selectEl.appendChild(opt);
  });
}

async function openInvModalAndPopulate() {
  const tableSel = document.getElementById('invm-table');
  const whSel = document.getElementById('invm-warehouse-col');
  const qtySel = document.getElementById('invm-qty-col');

  // 1) 테이블 목록
  try {
    const tables = await fetchTables();
    fillSelect(tableSel, tables, '(테이블 선택)');
  } catch (e) {
    alert('테이블 목록을 불러오지 못했습니다: ' + e.message);
  }

  // 2) 테이블 선택 시 컬럼 채우기
  tableSel.onchange = async () => {
    const t = tableSel.value;
    if (!t) {
      fillSelect(whSel, [], '(먼저 테이블을 선택하세요)');
      fillSelect(qtySel, [], '(먼저 테이블을 선택하세요)');
      return;
    }
    try {
      const cols = await fetchTableColumns(t);
      fillSelect(whSel, cols, '(창고 컬럼 선택)');
      fillSelect(qtySel, cols, '(재고(수량) 컬럼 선택)');
    } catch (e) {
      alert('컬럼 정보를 불러오지 못했습니다: ' + e.message);
    }
  };

  showInvModal();
}

// ===== 기존 인라인 버튼 → 모달 오픈으로 대체 =====
document.getElementById('inv-open-modal')?.addEventListener('click', openInvModalAndPopulate);
document.getElementById('inv-modal-close')?.addEventListener('click', hideInvModal);

// ===== 모달 적용(실행) 버튼: 기존 inv-run 로직 호출/대체 =====
document.getElementById('inv-modal-run')?.addEventListener('click', async () => {
  const table = document.getElementById('invm-table')?.value;
  const warehouseCol = document.getElementById('invm-warehouse-col')?.value;
  const qtyCol = document.getElementById('invm-qty-col')?.value;

  if (!table || !warehouseCol || !qtyCol) {
    alert('테이블/창고/재고 컬럼을 모두 선택하세요.');
    return;
  }

  hideInvModal();

  try {
    // 1) 백엔드 요약 API (권장)
    const res = await fetch(`${BACKEND_BASE_URL}/inventory/summary`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        table_name: table,
        warehouse_col: warehouseCol,
        qty_col: qtyCol,
        top_n: 100
      })
    });
    const js = await res.json();
    if (!res.ok) throw new Error(js.detail || 'inventory/summary failed');

    renderInventorySummary(js);

  } catch (e) {
    console.warn('[inventory/summary] 실패, preview로 폴백 →', e.message);

    try {
      // 2) 폴백: preview 엔드포인트 (쿼리x, path 파라미터!)
      const res2 = await fetch(
        `${BACKEND_BASE_URL}/table/${encodeURIComponent(table)}/preview?limit=5000`
      );
      const js2 = await res2.json();
      if (!res2.ok) throw new Error(js2.detail || 'preview fetch failed');

      let rows = js2.rows || [];
      if (Array.isArray(rows) && rows.length && Array.isArray(rows[0]) && Array.isArray(js2.columns)) {
        rows = rows.map(r => Object.fromEntries(js2.columns.map((c, i) => [c, r[i]])));
      }

      const byWh = new Map();
      let total = 0; // ← ✅ 주석은 이렇게 쓰세요 (또는 /* ... */)

      for (const r of rows) {
        const w = String(r?.[warehouseCol] ?? '(null)');
        const q = Number(r?.[qtyCol]);
        if (!Number.isFinite(q)) continue;
        total += q;
        byWh.set(w, (byWh.get(w) || 0) + q);
      }

      const top = [...byWh.entries()]
        .sort((a,b) => b[1]-a[1])
        .slice(0, 100)
        .map(([warehouse, qty]) => ({ warehouse, qty }));

      renderInventorySummary({
        table,
        warehouse_col: warehouseCol,
        qty_col: qtyCol,
        total,
        rows: top
      });

    } catch (e2) {
      alert('재고 요약에 실패했습니다: ' + e2.message);
    }
  }
});

// ================== 재고 요약 통합 모듈 ==================
(function inventorySummaryModuleUnified(){
  const modal     = document.getElementById('inventoryModal');
  const btnOpen   = document.getElementById('btnInventorySummary');
  const btnConfirm= document.getElementById('inv-btnConfirm');
  const btnCancel = document.getElementById('inv-btnCancel');
  const tableSel  = document.getElementById('inv-tableSelect');
  const prodSel   = document.getElementById('inv-productColSel');
  const whSel     = document.getElementById('inv-warehouseColSel');
  const qtySel    = document.getElementById('inv-qtyColSel');
  const chartArea = document.getElementById('inventoryChartsArea');

  let chartInstances = {};

  const toNum = (v)=> {
    const n = Number(String(v ?? '').replace(/[, ]/g,''));
    return Number.isFinite(n) ? n : 0;
  };

  // ✅ 버튼 클릭 → 모달 오픈
  btnOpen.addEventListener('click', async ()=>{
    modal.style.display = 'flex';
    document.body.style.overflow = 'hidden';
    try{
      const js = await fetchJSON('GET', `${BACKEND_BASE_URL}/tables`);
      tableSel.innerHTML = `<option value="">(테이블 선택)</option>` + 
        (js?.tables??[]).map(t=>`<option value="${t}">${t}</option>`).join('');
    }catch(e){ alert('테이블 목록 불러오기 실패'); }
  });

  btnCancel.addEventListener('click', ()=>{
    modal.style.display = 'none';
    document.body.style.overflow = '';
  });

  tableSel.addEventListener('change', async ()=>{
    const tname = tableSel.value;
    if(!tname) return;
    try{
      const js = await fetchJSON('POST', `${BACKEND_BASE_URL}/table-columns`, { table_name: tname });
      const optHTML = (js?.columns??[]).map(c=>`<option value="${c}">${c}</option>`).join('');
      prodSel.innerHTML = optHTML;
      whSel.innerHTML   = optHTML;
      qtySel.innerHTML  = optHTML;
    }catch(e){ alert('컬럼 불러오기 실패'); }
  });

  // ✅ 확인 버튼 → 두 모듈 순차 실행
  btnConfirm.addEventListener('click', async ()=>{
    const table  = tableSel.value;
    const prodCol= prodSel.value;
    const whCol  = whSel.value;
    const qtyCols= Array.from(qtySel.selectedOptions||[]).map(o=>o.value);
    if(!table||!prodCol||!whCol||!qtyCols.length){ alert('모든 항목을 선택하세요.'); return; }

    modal.style.display = 'none';
    document.body.style.overflow = '';

    try{
      const js = await fetchJSON('POST', `${BACKEND_BASE_URL}/table-preview`, {
        table_name: table, max_rows: 10000
      });
      const data = js?.rows??[];
      if(!data.length) return alert('데이터가 없습니다.');

    chartArea.innerHTML = '';
    Object.values(chartInstances).forEach(ch=>{ try{ch.destroy();}catch(_){} });
    chartInstances = {};

    // ✅ 4열 그리드로 고정
    chartArea.style.display = 'grid';
    chartArea.style.gridTemplateColumns = 'repeat(4, minmax(0, 1fr))';
    chartArea.style.gap = '12px';

    // ① 제품별 상세
    await drawSummaryDetailed(data, prodCol, whCol, qtyCols, '①');
    // ② 창고 총합
    await drawSummaryTotal(data, prodCol, whCol, qtyCols, '②');

    }catch(e){ console.error(e); alert('데이터 불러오기 실패'); }
  });

  // ✅ 제품별 상세
  async function drawSummaryDetailed(data, prodCol, whCol, qtyCols, tag){
    const warehouses = [...new Set(data.map(r=>r?.[whCol]))].filter(Boolean);
    warehouses.forEach(wh=>{
      const whDiv = document.createElement('div');
      whDiv.className = 'inv-card';
      whDiv.innerHTML = `
        <h4 style="margin-bottom:.5rem;">${tag} - ${wh}</h4>
        <select class="inv-productSel" style="margin-bottom:.5rem;"></select>
        <div style="position:relative;width:100%;height:240px;">
          <canvas></canvas>
        </div>`;
      chartArea.appendChild(whDiv);

      const productSel = whDiv.querySelector('.inv-productSel');
      const canvas = whDiv.querySelector('canvas');
      const ctx = canvas.getContext('2d');

      const products = [...new Set(data.filter(r=>r?.[whCol]===wh).map(r=>r?.[prodCol]))]
        .filter(v=>v !== null && v !== undefined && v !== '')
        .map(v=>String(v));
      productSel.innerHTML = products.map(p=>`<option value="${esc(p)}">${esc(p)}</option>`).join('');

      // function drawChart(product){
      //   if(chartInstances[wh]){ chartInstances[wh].destroy(); delete chartInstances[wh]; }
      //   const rows = data.filter(r=>r?.[whCol]===wh && r?.[prodCol]===product);
      //   const totalSum = rows.reduce((acc,row)=>{
      //     return acc + qtyCols.reduce((sum,c)=>sum+toNum(row?.[c]),0);
      //   },0);
      //   const chart = new Chart(ctx,{
      //     type:'bar',
      //     data:{ labels:['총재고합계'], datasets:[{ label:`${product}`, data:[totalSum], backgroundColor:'rgba(99,102,241,0.5)', borderColor:'rgba(99,102,241,1)', borderWidth:1 }]},
      //     options:{ responsive:true, maintainAspectRatio:false, scales:{y:{beginAtZero:true}} }
      //   });
      //   chartInstances[`${wh}-${tag}`] = chart;
      // }



      function drawChart(product){
        const productKey = String(product ?? '');
        const warehouseKey = String(wh ?? '');
        const chartKey = `${warehouseKey}-${tag}`; // 창고별 차트 식별자
        const rows = data.filter(r => String(r?.[whCol] ?? '') === warehouseKey && String(r?.[prodCol] ?? '') === productKey);
        const totalSum = rows.reduce((acc,row)=>{
          return acc + qtyCols.reduce((sum,c)=>sum+toNum(row?.[c]),0);
        },0);

        const existing = chartInstances[chartKey];
        if (existing) {
          existing.data.labels = ['총재고합계'];
          const ds = existing.data.datasets?.[0];
          if (ds) {
            ds.label = productKey;
            ds.data = [totalSum];
            ds.backgroundColor = 'rgba(99,102,241,0.5)';
            ds.borderColor = 'rgba(99,102,241,1)';
            ds.borderWidth = 1;
          } else {
            existing.data.datasets = [{
              label: productKey,
              data: [totalSum],
              backgroundColor: 'rgba(99,102,241,0.5)',
              borderColor: 'rgba(99,102,241,1)',
              borderWidth: 1
            }];
          }
          existing.update();
          return;
        }

        const chart = new Chart(ctx,{
          type:'bar',
          data:{ labels:['총재고합계'], datasets:[{ label:productKey, data:[totalSum], backgroundColor:'rgba(99,102,241,0.5)', borderColor:'rgba(99,102,241,1)', borderWidth:1 }]},
          options:{ responsive:true, maintainAspectRatio:false, scales:{y:{beginAtZero:true}} }
        });
        chartInstances[chartKey] = chart;
      }

      if(products[0]) drawChart(products[0]);
      productSel.addEventListener('change',()=> drawChart(productSel.value));
    });
  }

  // ✅ 창고별 총합 (제품 선택 없음)
  async function drawSummaryTotal(data, prodCol, whCol, qtyCols, tag){
    const warehouses = [...new Set(data.map(r=>r?.[whCol]))].filter(Boolean);
    warehouses.forEach(wh=>{
      const whDiv = document.createElement('div');
      whDiv.className = 'inv-card';
      whDiv.innerHTML = `
        <h4 style="margin-bottom:.5rem;">${tag} - ${wh} (총합)</h4>
        <div style="position:relative;width:100%;height:240px;">
          <canvas></canvas>
        </div>`;
      chartArea.appendChild(whDiv);

      const canvas = whDiv.querySelector('canvas');
      const ctx = canvas.getContext('2d');

      const rows = data.filter(r=>r?.[whCol]===wh);
      const totalSum = rows.reduce((acc,row)=>{
        return acc + qtyCols.reduce((sum,c)=>sum+toNum(row?.[c]),0);
      },0);

      const chart = new Chart(ctx,{
        type:'bar',
        data:{
          labels:['총재고합계'],
          datasets:[{
            label:`창고 ${wh} 전체`,
            data:[totalSum],
            backgroundColor:'rgba(16,185,129,0.5)',
            borderColor:'rgba(16,185,129,1)',
            borderWidth:1
          }]
        },
        options:{ responsive:true, maintainAspectRatio:false, scales:{y:{beginAtZero:true}} }
      });
      chartInstances[`total-${wh}`] = chart;
    });
  }
})();
