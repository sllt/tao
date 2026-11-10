"use strict";
const $ = id => document.getElementById(id);
const labels = {idle:"空闲",running:"运行中",starting:"启动中",stopping:"停止中",stopped:"已停止",restarting:"重启中",preparing:"准备中",replacing:"替换中",handoff:"迁移中",outcome_unknown:"结果未知",unresponsive:"无响应",inspecting:"检查中"};
const state = {node:"",paused:false,tree:true,after:"",next:"",selected:"",actors:[],previous:new Map(),rates:new Map(),generation:0,request:0,filter:""};
function element(tag, className, text) { const e=document.createElement(tag); if(className)e.className=className;if(text!==undefined)e.textContent=text;return e; }
const count = n => Number(n||0).toLocaleString("zh-CN");
function bytes(n){n=Number(n||0);return n>=1048576?`${(n/1048576).toFixed(1)} MiB`:n>=1024?`${(n/1024).toFixed(1)} KiB`:`${n} B`;}
function elapsed(start){if(!start||start.startsWith("0001"))return "—";let seconds=Math.max(0,Math.floor((Date.now()-Date.parse(start))/1000));return seconds>=3600?`${Math.floor(seconds/3600)}h ${Math.floor(seconds%3600/60)}m`:seconds>=60?`${Math.floor(seconds/60)}m ${seconds%60}s`:`${seconds}s`;}
const ms = ns => `${(Number(ns||0)/1e6).toFixed(2)} ms`;
function query(extra={}){return new URLSearchParams({node:state.node,...extra}).toString();}
async function api(path){const response=await fetch(path,{cache:"no-store"});const body=await response.json();if(!response.ok)throw new Error(body.error||`HTTP ${response.status}`);return body;}
function connection(text,failed=false){$("connection").textContent=text;$("connection").className=`live${failed?" offline":""}`;}
function status(value){return element("span",`status ${value}`,labels[value]||value);}
function pidNumber(pid){return pid.split("/").pop();}
function actorName(pid){if(!pid)return "外部调用";const row=state.actors.find(a=>a.PID===pid);return row?row.Path:pid.replace("pid://","").replace(/\/[a-f0-9]{32}\//," / #");}
function actorOrder(){
  if(!state.tree)return state.actors.map(actor=>({actor,depth:0}));
  const byID=new Map(state.actors.map(actor=>[actor.PID,actor]));const children=new Map();
  for(const actor of state.actors){const parent=byID.has(actor.Parent)?actor.Parent:"";if(!children.has(parent))children.set(parent,[]);children.get(parent).push(actor);}
  const result=[],seen=new Set();
  function visit(parent,depth){for(const actor of children.get(parent)||[]){if(seen.has(actor.PID))continue;seen.add(actor.PID);result.push({actor,depth});visit(actor.PID,depth+1);}}
  visit("",0);for(const actor of state.actors)if(!seen.has(actor.PID))result.push({actor,depth:0});return result;
}
function renderActors(){
  const rows=[];const focused=document.activeElement?.dataset?.pid;
  for(const {actor,depth} of actorOrder()){
    const row=element("tr",state.selected===actor.PID?"selected":"");row.dataset.pid=actor.PID;row.tabIndex=0;row.setAttribute("aria-label",`${actor.Path} ${labels[actor.State]||actor.State}`);
    const name=element("td");name.style.paddingLeft=`${22+depth*15}px`;
    const title=element("span","actor-name");title.append(element("span","tree-dot",depth?"└":"◇"),document.createTextNode(actor.Path));
    const missingParent=actor.Parent&&!state.actors.some(a=>a.PID===actor.Parent);
    name.append(title,element("span","actor-sub",`${actor.Kind||actor.Metrics?.Behavior||"未标注类型"}${actor.Version?" @ "+actor.Version:""} · #${pidNumber(actor.PID)}${missingParent?" · 父 Actor 未加载":""}`));
    const st=element("td");st.append(status(actor.State));
    const mailbox=element("td","",`${count(actor.Mailbox)} / ${count(actor.MailboxCapacity)}`);const ratio=actor.MailboxCapacity?actor.Mailbox/actor.MailboxCapacity:0;
    const bar=element("div",`mailbar${ratio>.7?" hot":""}`),fill=element("span");fill.style.width=`${Math.min(100,ratio*100)}%`;bar.append(fill);mailbox.append(bar);
    row.append(name,st,mailbox,element("td","",state.rates.has(actor.PID)?state.rates.get(actor.PID).toFixed(1):"—"),element("td","",count(actor.Restarts)));rows.push(row);
  }
  $("actor-rows").replaceChildren(...rows);$("empty").classList.toggle("hidden",rows.length>0);
  if(focused){const row=rows.find(row=>row.dataset.pid===focused);if(row)row.focus({preventScroll:true});}
}
function record(title,subtitle){const e=element("div","record");e.append(element("div","record-main",title),element("div","record-sub",subtitle));return e;}
function renderSnapshot(data){
  const n=data.Node,host=data.Host;const sampled=Date.parse(n.Sampled),actors=n.Actors||[];
  state.rates.clear();for(const actor of actors){const previous=state.previous.get(actor.PID);if(actor.Metrics){if(previous&&sampled>previous.time&&actor.Metrics.Handled>=previous.count)state.rates.set(actor.PID,(actor.Metrics.Handled-previous.count)*1000/(sampled-previous.time));state.previous.set(actor.PID,{time:sampled,count:actor.Metrics.Handled});}}
  const visible=new Set(actors.map(a=>a.PID));for(const id of state.previous.keys())if(!visible.has(id))state.previous.delete(id);
  state.actors=actors;state.next=n.Next||"";
  $("node-name").textContent=n.Name;$("uptime").textContent=`已运行 ${elapsed(n.Created)}${n.Draining?" · 正在排空":""}${n.Stats.Stopping?" · 正在停止":""}`;
  $("actors-count").textContent=count(n.Stats.Actors);$("actor-status").textContent=`${count(n.Stats.RunningActors)} 个执行者 · ${count(n.Stats.Frozen)} 个冻结 · ${count(n.Stats.Unresponsive)} 个无响应`;
  $("mailbox-count").textContent=count(n.Stats.QueuedMessages);$("mailbox-note").textContent=`${count(n.Stats.Starting)} 个启动中 · ${count(n.Stats.Preparing)} 个准备中`;
  $("calls-count").textContent=count(n.Stats.PendingCalls);$("calls-note").textContent=`${count((n.Cycles||[]).length)} 个疑似等待环 · ${count(n.Stats.AsyncRequests)} 个异步请求/待处理结果`;
  $("heap-count").textContent=bytes(host.HeapAlloc);$("host-note").textContent=`${count(host.Goroutines)} goroutines · ${count(host.GC)} 次 GC · ${host.GoVersion}`;
  $("actor-total").textContent=count(n.Total);$("next-page").disabled=!state.next;$("first-page").disabled=!state.after;
  $("page-note").textContent=`显示 ${actors.length} / ${n.Total}${state.after?" · 后续页":""}${state.filter?" · 已筛选":""}`;renderActors();
  $("cycle-count").textContent=(n.Cycles||[]).length?"需进一步确认":"未发现等待环";
  $("cycles").replaceChildren(...(n.Cycles||[]).map(cycle=>element("div","",`疑似循环：${[...cycle,cycle[0]].map(actorName).join(" → ")}（DeferredReply 可能解除等待）`)));
  const calls=(n.Calls||[]).map(call=>record(`${actorName(call.Caller)} → ${call.Target||actorName(call.Owner)}`,`#${call.ID} · ${call.Asynchronous?"异步请求已运行":"已等待"} ${elapsed(call.Started)}${call.Deferred?" · 延后回复":""}${call.Relocated?" · 已迁移":""}`));
  $("calls").replaceChildren(...(calls.length?calls:[element("div","empty","没有等待中的 Call")]));
  if(n.CallsTotal>calls.length)$("calls").append(element("p","muted small",`仅展示 ${calls.length} / ${n.CallsTotal} 条请求；环检测不是完整视图。`));
  $("session-count").textContent=count(n.Network.Endpoint.Sessions);
  $("network-summary").textContent=`发送排队 ${bytes(n.Network.QueuedBytes)} · ${count(n.Network.QueuedMessages)} 条消息 · 容量预留 ${bytes(n.Network.Endpoint.ReservedBytes)}`;
  const peers=(n.Peers||[]).map(peer=>record(`${peer.Peer} / ${peer.Class}`,`${peer.Stats.Operations} 个操作 · ${peer.Stats.MuxStreams} 条流 · 发送排队 ${bytes(peer.Stats.MuxSendQueued)} · 未知结果 ${peer.Stats.UnknownResults}`));
  $("peers").replaceChildren(...(peers.length?peers:[element("div","empty","没有物理连接；本地 Actor 无需网络。")])) ;
  $("sample-time").textContent=`更新于 ${new Date(n.Sampled).toLocaleTimeString("zh-CN",{hour12:false})}`;
}
async function refreshDetail(){
  if(!state.selected)return;const selected=state.selected;
  try{
    const detail=await api(`/api/actor?${query({pid:selected})}`);if(selected!==state.selected)return;
    $("detail-empty").classList.add("hidden");$("detail-content").classList.remove("hidden");
    $("detail-path").textContent=detail.Path;$("detail-pid").textContent=detail.PID;
    $("detail-state").className=`status ${detail.State}`;$("detail-state").textContent=labels[detail.State]||detail.State;
    const m=detail.Metrics;const fields=[["Kind / 版本",`${detail.Kind||"—"} ${detail.Version||""}`],["父 Actor",detail.Parent?actorName(detail.Parent):"根 guardian"],["实例运行",elapsed(detail.Created)],["邮箱",`${detail.Mailbox} / ${detail.MailboxCapacity}`],["系统队列",count(detail.SystemQueue)],["路由 epoch",count(detail.Epoch)],["监督重启",count(detail.Restarts)],["已处理",m?count(m.Handled):"未采集"],["回调失败",m?count(m.Failed):"—"],["平均处理",m&&m.Handled?ms(m.ReceiveNanoseconds/m.Handled):"—"],["最长处理",m?ms(m.MaxReceiveNanoseconds):"—"],["邮箱高水位",m?count(m.MailboxHighWater):"—"],["邮箱拒绝",m?count(m.MailboxRejected):"—"],["正在处理",m?.CurrentMessage||"—"],["本次已运行",m?.CurrentMessage?elapsed(m.ReceiveStarted):"—"]];
    fields.push(["异步请求/待处理结果",count(detail.AsyncRequests)]);
    if(detail.MoveID)fields.push(["MoveID",detail.MoveID]);
    $("detail-fields").replaceChildren(...fields.flatMap(([label,value])=>[element("dt","",label),element("dd","",value)]));
    const relations=(detail.Relationships||[]).map(r=>element("div","",`${r.Kind}: ${actorName(r.From)} → ${actorName(r.To)}`));
    $("relationships").replaceChildren(...(relations.length?relations:[element("span","muted","无 Monitor / Link 关系")]));
    if(detail.RelationshipTotal>relations.length)$("relationships").append(element("p","muted",`仅展示 ${relations.length} / ${detail.RelationshipTotal} 条关系`));
  }catch(err){if(selected===state.selected){$("detail-state").textContent="实例不可用";$("detail-state").className="status stopped";$("inspect-result").textContent=err.message;}}
}
async function refresh(force=false){
  if(!state.node||state.paused&&!force)return;const generation=state.generation,request=++state.request;
  try{const data=await api(`/api/snapshot?${query({after:state.after,filter:state.filter})}`);if(generation!==state.generation||request!==state.request||state.paused&&!force)return;renderSnapshot(data);$("error").classList.add("hidden");connection(state.paused?"已暂停":"实时连接");await refreshDetail();}
  catch(err){if(generation===state.generation&&request===state.request){connection("连接中断",true);$("error").textContent=`更新失败，当前保留最后一次快照：${err.message}`;$("error").classList.remove("hidden");}}
}
async function tick(){if(!document.hidden)await refresh();setTimeout(tick,1000);}
function selectActor(pid){state.selected=pid;$("inspect-result").textContent="尚未请求";renderActors();refreshDetail();}
$("actor-rows").addEventListener("click",e=>{const row=e.target.closest("tr[data-pid]");if(row)selectActor(row.dataset.pid);});
$("actor-rows").addEventListener("keydown",e=>{if(e.key==="Enter"||e.key===" "){const row=e.target.closest("tr[data-pid]");if(row){e.preventDefault();selectActor(row.dataset.pid);}}});
$("pause").addEventListener("click",()=>{state.paused=!state.paused;state.generation++;$("pause").textContent=state.paused?"继续更新":"暂停更新";connection(state.paused?"已暂停":"实时连接");if(!state.paused)refresh();});
$("node").addEventListener("change",()=>{state.node=$("node").value;state.selected="";state.after="";state.previous.clear();state.generation++;$("detail-content").classList.add("hidden");$("detail-empty").classList.remove("hidden");refresh(true);});
$("tree-view").addEventListener("click",()=>{state.tree=true;$("tree-view").classList.add("selected");$("list-view").classList.remove("selected");renderActors();});
$("list-view").addEventListener("click",()=>{state.tree=false;$("list-view").classList.add("selected");$("tree-view").classList.remove("selected");renderActors();});
let filterTimer;$("filter").addEventListener("input",()=>{clearTimeout(filterTimer);filterTimer=setTimeout(()=>{state.filter=$("filter").value;state.after="";state.previous.clear();state.generation++;refresh(true);},250);});
$("first-page").addEventListener("click",()=>{state.after="";state.previous.clear();state.generation++;refresh(true);});
$("next-page").addEventListener("click",()=>{state.after=state.next;state.previous.clear();state.generation++;refresh(true);});
$("inspect").addEventListener("click",async()=>{const selected=state.selected;if(!selected)return;$("inspect").disabled=true;$("inspect-result").textContent="等待 Actor 执行边界…";try{const result=await api(`/api/inspect?${query({pid:selected})}`);if(selected===state.selected)$("inspect-result").textContent=JSON.stringify(result.Value,null,2);}catch(err){if(selected===state.selected)$("inspect-result").textContent=err.message;}finally{$("inspect").disabled=false;}});
(async()=>{try{const nodes=await api("/api/nodes");for(const node of nodes){const option=element("option","",node.Name);option.value=node.Name;$("node").append(option);}state.node=nodes[0]?.Name||"";tick();}catch(err){connection("连接失败",true);$("error").textContent=err.message;$("error").classList.remove("hidden");}})();
