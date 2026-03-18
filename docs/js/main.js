// ── Animated counters ──────────────────────────────────────────────────────
function animateCounter(el, target, duration = 1500, suffix = '') {
  const start = performance.now();
  const isFloat = target % 1 !== 0;
  const update = (now) => {
    const elapsed = now - start;
    const progress = Math.min(elapsed / duration, 1);
    const ease = 1 - Math.pow(1 - progress, 3);
    const value = Math.round(ease * target);
    el.textContent = value.toLocaleString(document.documentElement.lang === 'es' ? 'es-ES' : 'en-US') + suffix;
    if (progress < 1) requestAnimationFrame(update);
  };
  requestAnimationFrame(update);
}

// ── Intersection Observer for scroll animations ─────────────────────────────
const observerOptions = { threshold: 0.15, rootMargin: '0px 0px -50px 0px' };

const fadeObserver = new IntersectionObserver((entries) => {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      entry.target.classList.add('visible');
      fadeObserver.unobserve(entry.target);
    }
  });
}, observerOptions);

const counterObserver = new IntersectionObserver((entries) => {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      const el = entry.target;
      const target = parseInt(el.dataset.target);
      const suffix = el.dataset.suffix || '';
      animateCounter(el, target, 1800, suffix);
      counterObserver.unobserve(el);
    }
  });
}, { threshold: 0.5 });

// ── vCore Chart ─────────────────────────────────────────────────────────────
let vCoreChart = null;

function chartTheme() {
  const day = document.body.classList.contains('day-mode');
  return {
    grid:        day ? '#d0d7de' : '#30363d',
    tick:        day ? '#4a5568' : '#8b949e',
    limitLine:   day ? '#b07d00' : '#f2c500',
    tooltipBg:   day ? '#ffffff' : '#21262d',
    tooltipBorder: day ? '#d0d7de' : '#30363d',
    tooltipTitle: day ? '#1f2328' : '#e6edf3',
    tooltipBody:  day ? '#4a5568' : '#8b949e',
  };
}

function buildVCoreChart() {
  const canvas = document.getElementById('vcoreChart');
  if (!canvas || typeof Chart === 'undefined') return;

  const labels = ['V1\nPersistent', 'V2\n20 min', 'V3\n2 hours', 'V4\n4 hours ✓'];
  const data   = [900000, 850000, 80000, 45000];
  const colors = [
    'rgba(248, 81, 73, 0.85)',
    'rgba(255, 196, 50, 0.85)',
    'rgba(255, 165, 0, 0.85)',
    'rgba(214, 45, 97, 0.85)',
  ];
  const borderColors = ['#f85149', '#ffc432', '#ffa500', '#d62d61'];

  const limitPlugin = {
    id: 'limitLine',
    afterDraw(chart) {
      const t = chartTheme();
      const { ctx, chartArea, scales } = chart;
      const y = scales.y.getPixelForValue(100000);
      ctx.save();
      ctx.beginPath();
      ctx.setLineDash([8, 4]);
      ctx.strokeStyle = t.limitLine;
      ctx.lineWidth = 2;
      ctx.moveTo(chartArea.left, y);
      ctx.lineTo(chartArea.right, y);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = t.limitLine;
      ctx.font = '12px monospace';
      ctx.fillText(typeof t === 'function' ? t('chart.limit') : 'Free tier limit: 100,000', chartArea.left + 8, y - 8);
      ctx.restore();
    }
  };

  const t = chartTheme();
  vCoreChart = new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'vCore-seconds / month',
        data,
        backgroundColor: colors,
        borderColor: borderColors,
        borderWidth: 2,
        borderRadius: 6,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 1200, easing: 'easeOutQuart' },
      scales: {
        x: {
          ticks: { color: t.tick, font: { family: 'monospace', size: 12 } },
          grid: { color: t.grid },
        },
        y: {
          type: 'logarithmic',
          ticks: {
            color: t.tick,
            font: { family: 'monospace', size: 11 },
            callback: (v) => v >= 1000 ? (v / 1000).toLocaleString(document.documentElement.lang === 'es' ? 'es-ES' : 'en-US') + 'k' : v,
          },
          grid: { color: t.grid },
        }
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: t.tooltipBg,
          borderColor: t.tooltipBorder,
          borderWidth: 1,
          titleColor: t.tooltipTitle,
          bodyColor: t.tooltipBody,
          callbacks: {
            label: (ctx) => ' ' + ctx.raw.toLocaleString(document.documentElement.lang === 'es' ? 'es-ES' : 'en-US') + ' vCore-s/mo',
          }
        }
      }
    },
    plugins: [limitPlugin]
  });
}

function updateChartTheme() {
  if (!vCoreChart) return;
  const t = chartTheme();
  vCoreChart.options.scales.x.ticks.color       = t.tick;
  vCoreChart.options.scales.x.grid.color        = t.grid;
  vCoreChart.options.scales.y.ticks.color       = t.tick;
  vCoreChart.options.scales.y.grid.color        = t.grid;
  vCoreChart.options.plugins.tooltip.backgroundColor = t.tooltipBg;
  vCoreChart.options.plugins.tooltip.borderColor     = t.tooltipBorder;
  vCoreChart.options.plugins.tooltip.titleColor      = t.tooltipTitle;
  vCoreChart.options.plugins.tooltip.bodyColor       = t.tooltipBody;
  vCoreChart.update('none');
}

// Trigger chart only when visible
const chartObserver = new IntersectionObserver((entries) => {
  entries.forEach(entry => {
    if (entry.isIntersecting && !vCoreChart) {
      buildVCoreChart();
      chartObserver.unobserve(entry.target);
    }
  });
}, { threshold: 0.3 });

// ── Real Azure Monitor consumption data (Mar 15–18, 2026) ────────────────────
const CONSUMPTION_DATA = [
  ['15/03 13:06',99501],['15/03 13:21',99416],['15/03 13:51',98924],
  ['15/03 14:06',98832],['15/03 14:21',97803],['15/03 14:36',97768],
  ['15/03 14:51',97203],['15/03 15:06',96539],['15/03 15:21',95974],
  ['15/03 15:36',95320],['15/03 15:51',94743],['15/03 16:06',94089],
  ['15/03 16:21',93520],['15/03 16:36',92858],['15/03 16:51',92289],
  ['15/03 17:06',91630],['15/03 17:21',91067],['15/03 17:36',90402],
  ['15/03 17:51',89840],['15/03 18:06',89179],['15/03 18:21',88610],
  ['15/03 18:36',87966],['15/03 18:51',87389],['15/03 19:06',86721],
  ['15/03 19:21',86153],['15/03 19:36',85505],['15/03 19:51',84929],
  ['15/03 20:06',84270],['15/03 20:21',83710],['15/03 20:36',83042],
  ['15/03 20:51',82482],['15/03 21:06',81824],['15/03 21:21',81255],
  ['15/03 21:36',80594],['15/03 21:51',80019],['15/03 22:06',79368],
  ['15/03 22:21',78806],['15/03 22:36',78138],['15/03 22:51',77579],
  ['15/03 23:06',77162],['15/03 23:21',76662],['15/03 23:36',76331],
  ['15/03 23:51',75848],['16/03 00:06',75436],
  // ↑ Night pause 00:06–07:21 (~581 vCore-s consumed)
  ['16/03 07:21',74855],['16/03 07:51',74523],['16/03 08:06',74188],
  ['16/03 08:21',73288],['16/03 08:36',72963],['16/03 08:51',72564],
  ['16/03 09:06',71983],['16/03 09:21',71659],['16/03 09:36',71321],
  ['16/03 09:51',70833],['16/03 10:06',70417],['16/03 10:21',70099],
  ['16/03 10:51',69680],['16/03 11:06',69109],['16/03 11:21',68782],
  ['16/03 11:36',68447],['16/03 11:51',67959],['16/03 12:06',67553],
  ['16/03 12:21',67216],['16/03 12:36',66803],['16/03 12:51',66232],
  ['16/03 13:06',65907],['16/03 13:21',65576],['16/03 13:36',65095],
  ['16/03 13:51',64686],['16/03 14:06',63941],['16/03 14:21',63368],
  ['16/03 14:36',63037],['16/03 15:06',62220],['16/03 15:36',61813],
  ['16/03 15:51',61062],['16/03 16:21',60247],['16/03 16:51',59343],
  ['16/03 17:06',58926],['16/03 17:21',58603],['16/03 17:36',58191],
  ['16/03 18:06',57276],['16/03 18:21',56936],['16/03 18:36',56440],
  ['16/03 18:51',55708],['16/03 19:06',55299],['16/03 19:21',54807],
  ['16/03 19:36',54319],['16/03 19:51',53746],['16/03 20:06',53172],
  ['16/03 20:21',52757],['16/03 20:36',52423],['16/03 20:51',52013],
  ['16/03 21:06',51529],['16/03 21:51',51367],['16/03 22:06',50948],
  ['16/03 22:51',50537],
  // ↑ Night pause 22:51–09:06 (~167 vCore-s consumed)
  ['17/03 09:06',50370],['17/03 11:06',49881],['17/03 11:21',49796],
  ['17/03 13:21',49433],['17/03 15:06',48973],['17/03 16:06',48564],
  ['17/03 17:06',47992],['17/03 17:51',47750],['17/03 18:51',47421],
  ['17/03 19:06',47340],['17/03 22:06',46848],['17/03 23:06',46683],
  ['17/03 23:51',46267],['18/03 00:06',45779],
  // ↑ Night pause 00:06–11:21 (~88 vCore-s consumed)
  ['18/03 11:21',45691],['18/03 15:06',45438],['18/03 15:21',45114],
];

function buildRealConsumptionChart() {
  const canvas = document.getElementById('realConsumptionChart');
  if (!canvas || typeof Chart === 'undefined') return;

  const labels = CONSUMPTION_DATA.map(d => d[0]);
  const values = CONSUMPTION_DATA.map(d => d[1]);
  const t = chartTheme();

  // Gradient fill
  const ctx2d = canvas.getContext('2d');
  const grad = ctx2d.createLinearGradient(0, 0, 0, canvas.offsetHeight || 280);
  grad.addColorStop(0, 'rgba(214,45,97,0.22)');
  grad.addColorStop(1, 'rgba(214,45,97,0)');

  const limitPlugin = {
    id: 'realLimitLine',
    afterDraw(chart) {
      const th = chartTheme();
      const { ctx, chartArea, scales } = chart;
      const y = scales.y.getPixelForValue(100000);
      if (y < chartArea.top) return;
      ctx.save();
      ctx.beginPath();
      ctx.setLineDash([6, 3]);
      ctx.strokeStyle = th.limitLine;
      ctx.lineWidth = 1.5;
      ctx.moveTo(chartArea.left, y);
      ctx.lineTo(chartArea.right, y);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = th.limitLine;
      ctx.font = '11px monospace';
      ctx.fillText('100k limit', chartArea.right - 78, y - 5);
      ctx.restore();
    }
  };

  new Chart(canvas, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Free amount remaining',
        data: values,
        borderColor: '#d62d61',
        backgroundColor: grad,
        borderWidth: 2,
        fill: true,
        tension: 0.25,
        pointRadius: 0,
        pointHoverRadius: 5,
        pointHoverBackgroundColor: '#d62d61',
        pointHoverBorderColor: '#fff',
        pointHoverBorderWidth: 2,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 1000, easing: 'easeOutQuart' },
      scales: {
        x: {
          ticks: {
            color: t.tick,
            font: { family: 'monospace', size: 11 },
            maxRotation: 0,
            callback(value, index) {
              const lbl = labels[index];
              if (!lbl) return '';
              const date = lbl.split(' ')[0];
              if (index === 0) return date;
              const prev = labels[index - 1];
              return (prev && prev.split(' ')[0] !== date) ? date : '';
            }
          },
          grid: { color: t.grid }
        },
        y: {
          min: 40000,
          max: 105000,
          ticks: {
            color: t.tick,
            font: { family: 'monospace', size: 11 },
            callback: v => (v / 1000).toFixed(0) + 'k'
          },
          grid: { color: t.grid }
        }
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: t.tooltipBg,
          borderColor: t.tooltipBorder,
          borderWidth: 1,
          titleColor: t.tooltipTitle,
          bodyColor: t.tooltipBody,
          callbacks: {
            title: items => labels[items[0].dataIndex],
            label: ctx => ' ' + ctx.raw.toLocaleString(document.documentElement.lang === 'es' ? 'es-ES' : 'en-US') + ' vCore-s remaining'
          }
        }
      }
    },
    plugins: [limitPlugin]
  });
}

// ── Schema tabs ─────────────────────────────────────────────────────────────
function initSchemaTabs() {
  document.querySelectorAll('.schema-tabs').forEach(tabGroup => {
    const buttons = tabGroup.querySelectorAll('.tab-btn');
    const panels  = tabGroup.querySelectorAll('.tab-panel');
    buttons.forEach((btn, i) => {
      btn.addEventListener('click', () => {
        buttons.forEach(b => b.classList.remove('active'));
        panels.forEach(p => p.classList.remove('active'));
        btn.classList.add('active');
        panels[i].classList.add('active');
      });
    });
  });
}

// ── Copy to clipboard ───────────────────────────────────────────────────────
function initCopyButtons() {
  document.querySelectorAll('.copy-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const codeBlock = btn.closest('.code-block').querySelector('code');
      navigator.clipboard.writeText(codeBlock.textContent).then(() => {
        btn.innerHTML = '<i class="fas fa-check"></i>';
        btn.classList.add('copied');
        setTimeout(() => { btn.innerHTML = '<i class="fas fa-copy"></i>'; btn.classList.remove('copied'); }, 2000);
      });
    });
  });
}

// ── Typewriter hero ──────────────────────────────────────────────────────────
function typewriter(el, text, speed = 50) {
  el.textContent = '';
  let i = 0;
  const type = () => {
    if (i < text.length) {
      el.textContent += text[i++];
      setTimeout(type, speed);
    } else {
      el.classList.add('done');
    }
  };
  type();
}

// ── Navbar scroll effect ────────────────────────────────────────────────────
function initNavbar() {
  const nav = document.querySelector('.navbar');
  window.addEventListener('scroll', () => {
    nav.classList.toggle('scrolled', window.scrollY > 50);
  }, { passive: true });

  // Active nav link on scroll
  const sections = document.querySelectorAll('section[id]');
  const navLinks = document.querySelectorAll('.nav-link');
  const spy = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        navLinks.forEach(a => a.classList.remove('active'));
        const link = document.querySelector(`.nav-link[href="#${entry.target.id}"]`);
        if (link) link.classList.add('active');
      }
    });
  }, { rootMargin: '-40% 0px -50% 0px' });
  sections.forEach(s => spy.observe(s));

  // Mobile menu
  const burger = document.getElementById('burger');
  const navMenu = document.getElementById('nav-menu');
  if (burger) {
    burger.addEventListener('click', () => {
      navMenu.classList.toggle('open');
      burger.classList.toggle('open');
    });
    navMenu.querySelectorAll('a').forEach(a => {
      a.addEventListener('click', () => {
        navMenu.classList.remove('open');
        burger.classList.remove('open');
      });
    });
  }
}

// ── Animated background particles ──────────────────────────────────────────
function initParticles() {
  const canvas = document.getElementById('particles');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  let W, H, particles;

  function resize() {
    W = canvas.width = canvas.offsetWidth;
    H = canvas.height = canvas.offsetHeight;
  }

  function createParticles() {
    particles = Array.from({ length: 60 }, () => ({
      x: Math.random() * W,
      y: Math.random() * H,
      r: Math.random() * 1.5 + 0.5,
      vx: (Math.random() - 0.5) * 0.3,
      vy: (Math.random() - 0.5) * 0.3,
      alpha: Math.random() * 0.5 + 0.1,
    }));
  }

  function draw() {
    ctx.clearRect(0, 0, W, H);
    particles.forEach(p => {
      p.x = (p.x + p.vx + W) % W;
      p.y = (p.y + p.vy + H) % H;
      ctx.beginPath();
      ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
      ctx.fillStyle = `rgba(214, 45, 97, ${p.alpha})`;
      ctx.fill();
    });
    // Draw connections
    for (let i = 0; i < particles.length; i++) {
      for (let j = i + 1; j < particles.length; j++) {
        const dx = particles[i].x - particles[j].x;
        const dy = particles[i].y - particles[j].y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 100) {
          ctx.beginPath();
          ctx.strokeStyle = `rgba(214, 45, 97, ${0.05 * (1 - dist / 100)})`;
          ctx.lineWidth = 0.5;
          ctx.moveTo(particles[i].x, particles[i].y);
          ctx.lineTo(particles[j].x, particles[j].y);
          ctx.stroke();
        }
      }
    }
    requestAnimationFrame(draw);
  }

  resize();
  createParticles();
  draw();
  window.addEventListener('resize', () => { resize(); createParticles(); });
}

// ── Mermaid init ─────────────────────────────────────────────────────────────
function initMermaid() {
  if (typeof mermaid === 'undefined') return;
  mermaid.initialize({
    startOnLoad: true,
    theme: 'dark',
    themeVariables: {
      primaryColor: '#21262d',
      primaryTextColor: '#e6edf3',
      primaryBorderColor: '#d62d61',
      lineColor: '#f2c500',
      secondaryColor: '#161b22',
      tertiaryColor: '#0d1117',
      edgeLabelBackground: '#161b22',
      clusterBkg: '#161b22',
      titleColor: '#e6edf3',
      nodeTextColor: '#e6edf3',
    },
    flowchart: { curve: 'basis', padding: 20 },
  });
}

// ── Haversine Calculator ─────────────────────────────────────────────────────
function initHaversineCalc() {
  const fields = ['hav-lat1','hav-lon1','hav-lat2','hav-lon2'].map(id => document.getElementById(id));
  if (!fields.every(Boolean)) return;

  function haversineKm(lat1, lon1, lat2, lon2) {
    const R = 6371;
    const dlat = (lat2 - lat1) * Math.PI / 180;
    const dlon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dlat/2)**2 +
              Math.cos(lat1*Math.PI/180) * Math.cos(lat2*Math.PI/180) * Math.sin(dlon/2)**2;
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  }

  function calcBearing(lat1, lon1, lat2, lon2) {
    const lat1r = lat1 * Math.PI/180, lat2r = lat2 * Math.PI/180;
    const dlonr = (lon2 - lon1) * Math.PI/180;
    const x = Math.sin(dlonr) * Math.cos(lat2r);
    const y = Math.cos(lat1r) * Math.sin(lat2r) - Math.sin(lat1r) * Math.cos(lat2r) * Math.cos(dlonr);
    return (Math.atan2(x, y) * 180 / Math.PI + 360) % 360;
  }

  function bearingToDir(b) {
    const keys = ['dir.N','dir.NE','dir.E','dir.SE','dir.S','dir.SW','dir.W','dir.NW'];
    return typeof t === 'function' ? t(keys[Math.round(b / 45) % 8]) : keys[Math.round(b / 45) % 8];
  }

  function update() {
    const [lat1, lon1, lat2, lon2] = fields.map(f => parseFloat(f.value));
    if ([lat1, lon1, lat2, lon2].some(isNaN)) return;

    const dist = haversineKm(lat1, lon1, lat2, lon2);
    const bearing = calcBearing(lat1, lon1, lat2, lon2);
    const speedKmh = (dist / 30) * 3600;

    document.getElementById('hav-dist').textContent    = dist.toFixed(1) + ' km';
    document.getElementById('hav-bearing').textContent = bearing.toFixed(1) + '°';
    document.getElementById('hav-dir').textContent     = bearingToDir(bearing);
    document.getElementById('hav-speed').textContent   = speedKmh.toFixed(0) + ' km/h';

    const needle = document.getElementById('compass-needle-group');
    if (needle) needle.setAttribute('transform', `rotate(${bearing.toFixed(1)},100,100)`);

    const caption = document.getElementById('compass-caption');
    if (caption) caption.textContent = `${bearingToDir(bearing)} · ${bearing.toFixed(1)}°`;
  }

  fields.forEach(f => f.addEventListener('input', update));

  document.querySelectorAll('.preset-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.preset-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      document.getElementById('hav-lat1').value = btn.dataset.lat1;
      document.getElementById('hav-lon1').value = btn.dataset.lon1;
      document.getElementById('hav-lat2').value = btn.dataset.lat2;
      document.getElementById('hav-lon2').value = btn.dataset.lon2;
      update();
    });
  });

  update(); // initial calc
}

// ── Theme toggle (day / night) ───────────────────────────────────────────────
function initThemeToggle() {
  const btn  = document.getElementById('theme-toggle');
  const icon = document.getElementById('theme-icon');
  if (!btn) return;

  // 'day-mode' class activates the light theme.
  const apply = (day) => {
    document.body.classList.toggle('day-mode', day);
    icon.className = day ? 'fas fa-moon' : 'fas fa-sun';
  };

  // Priority: saved preference → system preference (prefers-color-scheme)
  const saved = localStorage.getItem('theme');
  const systemDay = window.matchMedia('(prefers-color-scheme: light)').matches;
  apply(saved ? saved === 'day' : systemDay);

  btn.addEventListener('click', () => {
    const isDay = document.body.classList.toggle('day-mode');
    icon.className = isDay ? 'fas fa-moon' : 'fas fa-sun';
    localStorage.setItem('theme', isDay ? 'day' : 'night');
    updateChartTheme();
  });
}

// ── Boot ─────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  initMermaid();
  initThemeToggle();
  initNavbar();
  initParticles();
  initSchemaTabs();
  initCopyButtons();
  initHaversineCalc();

  // Fade-in elements
  document.querySelectorAll('.fade-in').forEach(el => fadeObserver.observe(el));

  // Animated counters
  document.querySelectorAll('.counter').forEach(el => counterObserver.observe(el));

  // Chart triggers
  const chartEl = document.getElementById('vcoreChart');
  if (chartEl) chartObserver.observe(chartEl);

  const realChartEl = document.getElementById('realConsumptionChart');
  if (realChartEl) {
    const realObserver = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          buildRealConsumptionChart();
          realObserver.unobserve(entry.target);
        }
      });
    }, { threshold: 0.2 });
    realObserver.observe(realChartEl);
  }

  // Typewriter
  const tw = document.querySelector('.typewriter');
  if (tw) {
    const finalText = tw.dataset.text || tw.textContent;
    tw.dataset.text = finalText;
    setTimeout(() => typewriter(tw, finalText, 40), 300);
  }
});
