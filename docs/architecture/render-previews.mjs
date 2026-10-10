import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

// 简图用于 README 和图册预览，详细交互图由 render.mjs 单独生成。
const root = dirname(fileURLToPath(import.meta.url));
const outputs = {
  overview: 'overview',
  architecture: 'research-pipeline-architecture', dataflow: 'dataflow-research-lineage',
  workflow: 'workflow-research-task', sequence: 'sequence-research-run', lifecycle: 'lifecycle-research-run',
};
const titles = {overview:'总体概览', architecture:'总体架构', dataflow:'数据追溯', workflow:'研究流程', sequence:'调用时序', lifecycle:'运行状态'};
const colors = {core:'#2563eb', ai:'#7c3aed', data:'#087f8c', check:'#15803d', human:'#b45309', error:'#be123c'};
const escape = value => String(value ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
const text = (x,y,value,size=14,color='#475569',weight=400,anchor='start') => `<text x="${x}" y="${y}" font-size="${size}" fill="${color}" font-weight="${weight}" text-anchor="${anchor}">${escape(value)}</text>`;
function anchors(a,b) {
  const ax=a.x+a.w/2, ay=a.y+a.h/2, bx=b.x+b.w/2, by=b.y+b.h/2;
  if (Math.abs(bx-ax)>Math.abs(by-ay)) return bx>ax ? [[a.x+a.w,ay],[b.x,by]] : [[a.x,ay],[b.x+b.w,by]];
  return by>ay ? [[ax,a.y+a.h],[bx,b.y]] : [[ax,a.y],[bx,b.y+b.h]];
}
function edgeSvg(points,label,optional=false,attrs='',labelPos) {
  const [a,b] = [points[0],points.at(-1)];
  const pos=labelPos || [(a[0]+b[0])/2,(a[1]+b[1])/2-9];
  const width=Array.from(label).length*12+14;
  return `<g class="edge" ${attrs}><path d="${points.map((p,i)=>(i?'L':'M')+p.join(',')).join(' ')}" fill="none" stroke="${optional?'#8b5cf6':'#64748b'}" stroke-width="2" ${optional?'stroke-dasharray="6 5"':''} marker-end="url(#arrow)"/>${label?`<rect x="${pos[0]-width/2}" y="${pos[1]-13}" width="${width}" height="19" rx="5" fill="#f8fafc"/>${text(pos[0],pos[1]+1,label,12,'#475569',400,'middle')}`:''}</g>`;
}
function draw(d) {
  const index = Object.fromEntries(d.nodes.map(n=>[n.id,n]));
  const invalid=d.edges.filter(e=>!index[e.from_id]||!index[e.to_id]);
  if(invalid.length) throw new Error('存在未定义的连线节点');
  let body=`<rect width="${d.width}" height="${d.height}" rx="20" fill="#f8fafc"/>`;
  for(const g of d.groups||[]) body+=`<g class="module-group"><rect x="${g.x}" y="${g.y}" width="${g.w}" height="${g.h}" rx="16" fill="${g.fill}" stroke="#dbe3ed"/>${text(g.x+16,g.y+24,g.label,14,"#475569",700)}</g>`;
  if(d.messages) {
    for(const n of d.nodes) body+=`<line x1="${n.x+n.w/2}" x2="${n.x+n.w/2}" y1="${n.y+n.h}" y2="${d.height-25}" stroke="#cbd5e1" stroke-dasharray="5 6"/>`;
    for(const [from,to,label,y] of d.messages) {
      const ax=index[from].x+120,bx=index[to].x+120;
      const points=from===to?[[ax,y-12],[ax+65,y-12],[ax+65,y+12],[ax,y+12]]:[[ax,y],[bx,y]];
      body+=edgeSvg(points,label,false,`data-from="${from}" data-to="${to}"`,from===to?[ax+175,y+2]:undefined);
    }
  }
  for(const e of d.edges) body+=edgeSvg(e.points||anchors(index[e.from_id],index[e.to_id]),e.label,e.kind==='optional',`data-from="${e.from_id}" data-to="${e.to_id}"`,e.label_pos);
  for(const n of d.nodes) body+=`<g class="node" data-id="${n.id}" tabindex="0" role="button" aria-pressed="false" aria-label="${escape(n.label)}"><title>${escape(n.detail)}</title><rect x="${n.x}" y="${n.y}" width="${n.w}" height="${n.h}" rx="12" fill="white" stroke="${colors[n.kind]||colors.core}" stroke-width="1.5"/><rect x="${n.x}" y="${n.y+15}" width="4" height="${n.h-30}" rx="2" fill="${colors[n.kind]||colors.core}"/>${text(n.x+18,n.y+35,n.label,17,'#0f172a',700)}${text(n.x+18,n.y+62,n.sub,13)}<circle cx="${n.x+n.w-14}" cy="${n.y+14}" r="3" fill="${colors[n.kind]||colors.core}"/></g>`;
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${d.width} ${d.height}" role="img" aria-label="${escape(d.title)}"><defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="#64748b"/></marker></defs><style>text{font-family:"Microsoft YaHei","微软雅黑",sans-serif;font-style:normal}.node{cursor:pointer}.node:focus{outline:none}.node:focus-visible>rect:first-of-type{stroke-dasharray:4 3}.dim{opacity:.17}.highlight>rect:first-of-type{stroke-width:4}.edge{transition:opacity .15s}</style>${body}</svg>`;
}
const selected=process.argv.slice(2);
for(const kind of selected.length?selected:Object.keys(outputs)) {
  if(!outputs[kind])throw new Error('未知图类型：'+kind);
  const data=JSON.parse(readFileSync(join(root,'sources',kind+'.json'),'utf8'));
  const svg=draw(data), stem=outputs[kind];
  writeFileSync(join(root,stem+'.svg'),svg+'\n','utf8');


  console.log(kind+' → SVG 简图');
}
const cards=Object.keys(outputs).map(k=>`<a class="card" href="${outputs[k]}.html"><img src="${outputs[k]}.svg" alt="${titles[k]}预览"><h2>${titles[k]}</h2><p>${JSON.parse(readFileSync(join(root,'sources',k+'.json'),'utf8')).question}</p></a>`).join('');
writeFileSync(join(root,'index.html'),`<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>QuantWitness · 架构图册</title><style>*{box-sizing:border-box}body{margin:0;background:#f1f5f9;color:#0f172a;font:16px/1.8 "Microsoft YaHei","微软雅黑",sans-serif}main{max-width:1240px;margin:auto;padding:50px 24px}h1{font-size:40px;line-height:1.3}.eyebrow{color:#6d28d9;font-weight:bold;letter-spacing:2px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:24px;margin-top:35px}.card{display:block;background:white;border:1px solid #dbe3ed;border-radius:18px;padding:22px;color:inherit;text-decoration:none}.card:hover{border-color:#7c3aed}.card img{width:100%;aspect-ratio:1.6;object-fit:contain}h2{margin-bottom:6px}p{color:#475569}a{color:#2563eb}</style><main><div class="eyebrow">QUANTWITNESS</div><h1>让 AI 探索，<br>让研究有据可查。</h1><p>六张图，看懂从研究想法、历史数据，到回测与独立验证的全过程。下方为简图预览；点击直接进入详细交互版，查看完整模块、调用关系和说明。</p><a href="https://github.com/ljjtim/QuantWitness">返回 GitHub 与使用文档 →</a><section class="grid">${cards}</section><p>无需联网即可浏览已下载的图册；源码链接需要网络。图册不包含行情或研究结果。</p></main></html>\n`,'utf8');
