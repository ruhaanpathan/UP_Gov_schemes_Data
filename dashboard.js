// ═══ DATA-DRIVEN DASHBOARD v3.0 ═══
// All data from scraped portals. Impact scores COMPUTED, not hardcoded.
// SCHEMES and PIPELINE_META loaded from dashboard_data.js

const SECTOR_COLORS={"Women & Girl Child":"#ec4899","Social Security":"#f97316","Agriculture":"#22c55e","Employment & Industry":"#3b82f6","Education":"#a855f7","Health":"#06b6d4","Housing":"#ef4444","Food & Civil Supplies":"#84cc16"};
const VERDICT_MAP={"Major Success":{cls:"badge-major",icon:"🏆",color:"#10b981"},"Success":{cls:"badge-success",icon:"✅",color:"#4ade80"},"Moderate Success":{cls:"badge-moderate",icon:"👍",color:"#fbbf24"},"Mixed":{cls:"badge-mixed",icon:"⚠️",color:"#fb923c"},"Underperformed":{cls:"badge-under",icon:"❌",color:"#f87171"},"Too Early to Judge":{cls:"badge-early",icon:"🕐",color:"#94a3b8"}};

function animateCounters(){
  if(typeof PIPELINE_META!=='undefined'){
    const m=PIPELINE_META;
    document.querySelector('#stat-schemes .stat-num').dataset.target=m.total_schemes;
    document.querySelector('#stat-ben .stat-num').dataset.target=(m.total_beneficiaries_lakh/100).toFixed(2);
    document.querySelector('#stat-budget .stat-num').dataset.target=(m.total_budget_crore/100).toFixed(2);
    document.querySelector('#stat-sectors .stat-num').dataset.target=m.sectors;
    document.querySelector('#stat-score .stat-num').dataset.target=m.avg_impact;
    const ds=document.getElementById('dataStatus');
    if(ds) ds.innerHTML=`<p style="color:#10b981;margin-bottom:4px">🟢 <strong>LIVE SCRAPED DATA</strong> — ${m.generated_at}</p>
      <p style="color:#64748b;font-size:12px;margin:0">${m.data_note||'Impact scores computed from scraped data'}</p>`;
  }
  document.querySelectorAll('.stat-num').forEach(el=>{
    const target=parseFloat(el.dataset.target);const isInt=Number.isInteger(target);
    let current=0;const step=target/60;
    const timer=setInterval(()=>{current+=step;if(current>=target){current=target;clearInterval(timer)}el.textContent=isInt?Math.round(current):current.toFixed(2)},25)
  })
}

function initSectorCharts(){
  const sectors={};
  SCHEMES.forEach(s=>{
    if(!sectors[s.sector]) sectors[s.sector]={ben:0,bud:0,count:0,impact:0,reach:0};
    sectors[s.sector].ben+=s.beneficiaries_lakh;
    sectors[s.sector].bud+=s.budget_crore||0;
    sectors[s.sector].count++;
    sectors[s.sector].impact+=s.impact_score;
    sectors[s.sector].reach+=s.reach_percent||0;
  });
  const labels=Object.keys(sectors);
  const colors=labels.map(l=>SECTOR_COLORS[l]||'#888');

  new Chart(document.getElementById('chartSectorBen'),{type:'doughnut',data:{labels,datasets:[{data:labels.map(l=>Math.round(sectors[l].ben)),backgroundColor:colors,borderColor:'#0a0e1a',borderWidth:3}]},options:{responsive:true,plugins:{legend:{position:'bottom',labels:{color:'#94a3b8',padding:12,font:{size:11}}}}}});

  new Chart(document.getElementById('chartSectorBudget'),{type:'bar',data:{labels,datasets:[{data:labels.map(l=>sectors[l].bud),backgroundColor:colors.map(c=>c+'cc'),borderColor:colors,borderWidth:1,borderRadius:6}]},options:{indexAxis:'y',responsive:true,plugins:{legend:{display:false}},scales:{x:{ticks:{color:'#64748b',callback:v=>'₹'+v.toLocaleString()+'Cr'},grid:{color:'#1e293b'}},y:{ticks:{color:'#94a3b8',font:{size:11}},grid:{display:false}}}}});

  new Chart(document.getElementById('chartSectorCompare'),{type:'bar',data:{labels,datasets:[{label:'Avg Impact (/10)',data:labels.map(l=>(sectors[l].impact/sectors[l].count).toFixed(1)),backgroundColor:'#6366f1aa',borderRadius:4},{label:'Avg Reach (%/10)',data:labels.map(l=>(sectors[l].reach/sectors[l].count/10).toFixed(1)),backgroundColor:'#06b6d4aa',borderRadius:4}]},options:{responsive:true,plugins:{legend:{labels:{color:'#94a3b8'}}},scales:{x:{ticks:{color:'#64748b',font:{size:10}},grid:{display:false}},y:{ticks:{color:'#64748b'},grid:{color:'#1e293b'}}}}});

  const container=document.getElementById('sectorCards');
  labels.sort((a,b)=>sectors[b].ben-sectors[a].ben).forEach(s=>{
    const d=sectors[s];const avg_imp=(d.impact/d.count).toFixed(1);
    container.innerHTML+=`<div class="sector-card" style="border-left:3px solid ${SECTOR_COLORS[s]||'#888'}"><div class="sc-name"><span class="sc-dot" style="background:${SECTOR_COLORS[s]||'#888'}"></span>${s}</div><div class="sc-row"><span>Schemes</span><span class="sc-val">${d.count}</span></div><div class="sc-row"><span>Beneficiaries</span><span class="sc-val">${d.ben.toFixed(1)}L</span></div><div class="sc-row"><span>Budget</span><span class="sc-val">₹${d.bud.toLocaleString()} Cr</span></div><div class="sc-row"><span>Avg Impact</span><span class="sc-val">${avg_imp}/10</span></div></div>`
  })
}

function renderSchemes(list){
  const grid=document.getElementById('schemeGrid');grid.innerHTML='';
  list.forEach((s,i)=>{
    const v=VERDICT_MAP[s.verdict]||VERDICT_MAP["Mixed"];
    const scoreColor=s.impact_score>=7.5?'#10b981':s.impact_score>=5.0?'#fbbf24':'#ef4444';
    const srcBadge=s.data_source?`<span style="font-size:10px;color:#10b981;background:#10b98122;padding:2px 6px;border-radius:4px">🌐 ${s.data_source}</span>`:'';
    
    // Build score breakdown if available
    let breakdownHtml='';
    if(s.impact_components){
      const c=s.impact_components;
      breakdownHtml=`<div style="margin-top:8px;font-size:11px;color:#94a3b8">
        <div style="font-weight:600;margin-bottom:4px">Score Breakdown (computed):</div>
        <div>Scale: ${c.scale||0}/10 | Efficiency: ${c.efficiency||0}/10 | Disbursement: ${c.disbursement||0}/10</div>
        <div>Coverage: ${c.coverage||0}/10 | Longevity: ${c.longevity||0}/10</div>
      </div>`;
    }
    
    // Extra metrics for specific schemes
    let extraHtml='';
    if(s.empanelled_hospitals) extraHtml+=`<div class="sc-metric"><div class="val">${s.empanelled_hospitals.toLocaleString()}</div><div class="lbl">Hospitals</div></div>`;
    if(s.claims_settled_pct) extraHtml+=`<div class="sc-metric"><div class="val">${s.claims_settled_pct}%</div><div class="lbl">Claims Settled</div></div>`;
    if(s.disbursed_crore>0) extraHtml+=`<div class="sc-metric"><div class="val">₹${s.disbursed_crore>=1000?(s.disbursed_crore/1000).toFixed(1)+'K':Math.round(s.disbursed_crore)}</div><div class="lbl">Cr Disbursed</div></div>`;

    grid.innerHTML+=`<div class="scheme-card animate-in" style="animation-delay:${i*0.05}s" onclick="this.classList.toggle('expanded')">
      <div class="sc-header"><div class="sc-title">${s.name}</div><span class="sc-badge ${v.cls}">${v.icon} ${s.verdict}</span></div>
      <div class="sc-sector" style="color:${SECTOR_COLORS[s.sector]||'#888'}">${s.sector} • Launched ${s.launch_year} ${srcBadge}</div>
      <div class="sc-metrics">
        <div class="sc-metric"><div class="val">${s.beneficiaries_lakh}L</div><div class="lbl">Beneficiaries</div></div>
        <div class="sc-metric"><div class="val">₹${s.budget_crore>=1000?(s.budget_crore/1000).toFixed(1)+'K':Math.round(s.budget_crore)}</div><div class="lbl">Crore Budget</div></div>
        <div class="sc-metric"><div class="val" style="color:${scoreColor}">${s.impact_score}</div><div class="lbl">Impact /10</div></div>
        ${extraHtml}
      </div>
      <div class="sc-score-bar"><div class="sc-score-fill" style="width:${Math.min(s.reach_percent||0,100)}%;background:${scoreColor}"></div></div>
      <div class="sc-detail"><div class="sc-detail-inner">
        <p style="font-size:12px;color:#64748b">👥 ${s.target_group||'N/A'}${s.per_person_benefit?' • '+s.per_person_benefit:''}</p>
        ${s.description?'<p style="font-size:12px;color:#94a3b8;margin:6px 0">'+s.description+'</p>':''}
        ${s.achievements&&s.achievements.length?'<h4>✓ Achievements</h4><ul>'+s.achievements.map(a=>'<li>'+a+'</li>').join('')+'</ul>':''}
        ${s.challenges&&s.challenges.length?'<h4>✗ Challenges</h4><ul>'+s.challenges.map(c=>'<li>'+c+'</li>').join('')+'</ul>':''}
        ${breakdownHtml}
      </div></div>
    </div>`
  })
}

function initFilters(){
  const sectorSel=document.getElementById('filterSector');
  const verdictSel=document.getElementById('filterVerdict');
  [...new Set(SCHEMES.map(s=>s.sector))].forEach(s=>{sectorSel.innerHTML+=`<option value="${s}">${s}</option>`});
  [...new Set(SCHEMES.map(s=>s.verdict))].forEach(v=>{verdictSel.innerHTML+=`<option value="${v}">${v}</option>`});

  const applyFilters=()=>{
    let list=[...SCHEMES];
    const sec=sectorSel.value,ver=verdictSel.value,q=document.getElementById('searchBox').value.toLowerCase(),sort=document.getElementById('sortBy').value;
    if(sec!=='all') list=list.filter(s=>s.sector===sec);
    if(ver!=='all') list=list.filter(s=>s.verdict===ver);
    if(q) list=list.filter(s=>s.name.toLowerCase().includes(q));
    list.sort((a,b)=>b[sort]-a[sort]);
    renderSchemes(list)
  };
  sectorSel.onchange=verdictSel.onchange=document.getElementById('searchBox').oninput=document.getElementById('sortBy').onchange=applyFilters;
  renderSchemes([...SCHEMES].sort((a,b)=>b.impact_score-a.impact_score))
}

function initVerdictCharts(){
  const vc={};SCHEMES.forEach(s=>{vc[s.verdict]=(vc[s.verdict]||0)+1});
  new Chart(document.getElementById('chartVerdict'),{type:'pie',data:{labels:Object.keys(vc),datasets:[{data:Object.values(vc),backgroundColor:Object.keys(vc).map(v=>(VERDICT_MAP[v]||{}).color||'#666'),borderColor:'#0a0e1a',borderWidth:2}]},options:{responsive:true,plugins:{legend:{position:'bottom',labels:{color:'#94a3b8',padding:10,font:{size:11}}}}}});

  new Chart(document.getElementById('chartScatter'),{type:'scatter',data:{datasets:Object.keys(SECTOR_COLORS).filter(sec=>SCHEMES.some(s=>s.sector===sec)).map(sec=>({label:sec,data:SCHEMES.filter(s=>s.sector===sec).map(s=>({x:s.budget_crore,y:s.impact_score,r:Math.sqrt(s.beneficiaries_lakh)*3,label:s.short})),backgroundColor:(SECTOR_COLORS[sec]||'#888')+'99',borderColor:SECTOR_COLORS[sec]||'#888',pointRadius:SCHEMES.filter(s=>s.sector===sec).map(s=>Math.max(5,Math.sqrt(s.beneficiaries_lakh)*2))}))},options:{responsive:true,plugins:{legend:{labels:{color:'#94a3b8',font:{size:10}}},tooltip:{callbacks:{label:ctx=>`${ctx.raw.label}: ₹${ctx.raw.x.toLocaleString()}Cr, Impact ${ctx.raw.y}/10`}}},scales:{x:{title:{display:true,text:'Budget (₹ Crore)',color:'#64748b'},ticks:{color:'#64748b'},grid:{color:'#1e293b'}},y:{title:{display:true,text:'Impact Score (COMPUTED /10)',color:'#64748b'},ticks:{color:'#64748b'},grid:{color:'#1e293b'}}}}});

  const container=document.getElementById('verdictSummary');
  ["Major Success","Success","Moderate Success","Mixed","Underperformed","Too Early to Judge"].forEach(v=>{
    const items=SCHEMES.filter(s=>s.verdict===v);if(!items.length)return;
    const vm=VERDICT_MAP[v];
    container.innerHTML+=`<div class="verdict-card" style="border-left:3px solid ${vm.color}"><div class="vc-title">${vm.icon} ${v} (${items.length})</div><ul>${items.map(s=>`<li><strong>${s.short}</strong> — ${s.beneficiaries_lakh}L, Score ${s.impact_score}/10 <span style="color:#64748b;font-size:11px">[${s.data_source}]</span></li>`).join('')}</ul></div>`
  })
}

function initImpact(){
  // Generate impact summary dynamically from data
  const grid=document.getElementById('impactGrid');
  const sectorData={};
  SCHEMES.forEach(s=>{
    if(!sectorData[s.sector]) sectorData[s.sector]={ben:0,schemes:[],icon:''};
    sectorData[s.sector].ben+=s.beneficiaries_lakh;
    sectorData[s.sector].schemes.push(s);
  });
  const icons={"Social Security":"👵","Health":"💊","Housing":"🏠","Agriculture":"🌾","Education":"🎓","Employment & Industry":"💼","Women & Girl Child":"👩","Food & Civil Supplies":"🍞"};
  
  Object.entries(sectorData).forEach(([sector, data])=>{
    const topScheme=data.schemes.sort((a,b)=>b.impact_score-a.impact_score)[0];
    const text=`${data.ben.toFixed(1)}L total beneficiaries across ${data.schemes.length} scheme(s). Top: ${topScheme.name} (Impact: ${topScheme.impact_score}/10). Data from: ${[...new Set(data.schemes.map(s=>s.data_source))].join(', ')}.`;
    grid.innerHTML+=`<div class="impact-card"><div class="ic-icon">${icons[sector]||'📊'}</div><div class="ic-title">${sector}</div><div class="ic-text">${text}</div></div>`
  })
}

function initFindings(){
  // Generate findings dynamically from data
  const grid=document.getElementById('findingsGrid');
  const sorted=[...SCHEMES].sort((a,b)=>b.impact_score-a.impact_score);
  const top3=sorted.slice(0,3).map(s=>`${s.short} (${s.impact_score}/10)`).join(', ');
  const bottom=sorted.slice(-1).map(s=>`${s.short} (${s.impact_score}/10)`).join(', ');
  const totalBen=SCHEMES.reduce((a,s)=>a+s.beneficiaries_lakh,0);
  const sources=[...new Set(SCHEMES.map(s=>s.data_source))];
  
  const findings=[
    {icon:"🏆",text:`<strong>Top Performers (by computed score):</strong> ${top3}`},
    {icon:"📊",text:`<strong>Total Reach:</strong> ${totalBen.toFixed(1)} Lakh beneficiaries across ${SCHEMES.length} schemes`},
    {icon:"🌐",text:`<strong>Data Sources:</strong> ${sources.join(', ')} — all numbers from live government portals`},
    {icon:"⚡",text:`<strong>Methodology:</strong> Impact scores computed using: 25% Scale + 20% Efficiency + 20% Disbursement + 20% Coverage + 15% Longevity. No hardcoded scores.`},
    {icon:"📉",text:`<strong>Lowest Performer:</strong> ${bottom}`},
  ];
  findings.forEach(f=>{grid.innerHTML+=`<div class="finding-card"><div class="fc-icon">${f.icon}</div><div class="fc-text">${f.text}</div></div>`})
}

document.addEventListener('DOMContentLoaded',()=>{
  animateCounters();initSectorCharts();initFilters();initVerdictCharts();initImpact();initFindings();
  const obs=new IntersectionObserver(entries=>{entries.forEach(e=>{if(e.isIntersecting)e.target.style.opacity='1'})},{threshold:0.1});
  document.querySelectorAll('.section').forEach(s=>obs.observe(s))
});
