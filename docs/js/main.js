// ── Animated counters ──────────────────────────────────────────────────────
function animateCounter(el, target, duration = 1500, suffix = '') {
  const start = performance.now();
  const isFloat = target % 1 !== 0;
  const update = (now) => {
    const elapsed = now - start;
    const progress = Math.min(elapsed / duration, 1);
    const ease = 1 - Math.pow(1 - progress, 3);
    const value = Math.round(ease * target);
    el.textContent = value.toLocaleString('es-ES') + suffix;
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

  const labels = ['V1\nPersistente', 'V2\n20 min', 'V3\n2 horas', 'V4\n4 horas ✓'];
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
      ctx.fillText('Límite gratuito: 100.000', chartArea.left + 8, y - 8);
      ctx.restore();
    }
  };

  const t = chartTheme();
  vCoreChart = new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'vCore-segundos / mes',
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
            callback: (v) => v >= 1000 ? (v / 1000).toLocaleString('es-ES') + 'k' : v,
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
            label: (ctx) => ' ' + ctx.raw.toLocaleString('es-ES') + ' vCore-s/mes',
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
    const dirs = ['Norte','Noreste','Este','Sureste','Sur','Suroeste','Oeste','Noroeste'];
    return dirs[Math.round(b / 45) % 8];
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

  // Default = dark/night. 'day-mode' class activates the light theme.
  const apply = (day) => {
    document.body.classList.toggle('day-mode', day);
    icon.className = day ? 'fas fa-moon' : 'fas fa-sun';
  };

  apply(localStorage.getItem('theme') === 'day');

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

  // Chart trigger
  const chartEl = document.getElementById('vcoreChart');
  if (chartEl) chartObserver.observe(chartEl);

  // Typewriter
  const tw = document.querySelector('.typewriter');
  if (tw) {
    const finalText = tw.dataset.text || tw.textContent;
    tw.dataset.text = finalText;
    setTimeout(() => typewriter(tw, finalText, 40), 300);
  }
});
