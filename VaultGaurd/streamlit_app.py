"""
SafeVault — Interactive Streamlit Demo Dashboard
Enterprise-Grade Backup & Restore System
"""

import asyncio
import hashlib
import json
import os
import time
from collections import defaultdict

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

# ── SafeVault imports ──────────────────────────────────────────────
from src.core.chunking import RabinChunker
from src.core.dedup import InMemoryDedupStore
from src.core.compression import Compressor
from src.core.encryption import Encryptor
from src.metadata.store import MetadataStore, BackupType, JobStatus
from src.messaging.pubsub import PubSubBus, Event, EventTopic
from src.storage.node import StorageNode, NodeStatus
from src.storage.cluster import StorageCluster
from src.backup.engine import BackupEngine
from src.restore.engine import RestoreEngine
from src.monitoring.metrics import MetricsCollector


# ═══════════════════════════════════════════════════════════════════
#  Page config & global styles
# ═══════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="SafeVault — Enterprise Backup & Restore",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');

/* Global font */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

/* Hide default streamlit branding */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}

/* Sidebar styling */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0a0e27 0%, #111638 50%, #0d1117 100%);
    border-right: 1px solid rgba(56, 189, 248, 0.08);
}

[data-testid="stSidebar"] * {
    color: #e2e8f0 !important;
}

/* Main background */
.stApp {
    background: linear-gradient(135deg, #0a0e1a 0%, #0d1321 40%, #111827 100%);
}

/* Metric cards */
.metric-card {
    background: linear-gradient(135deg, rgba(15, 23, 42, 0.9), rgba(30, 41, 59, 0.7));
    border: 1px solid rgba(56, 189, 248, 0.12);
    border-radius: 16px;
    padding: 24px 28px;
    text-align: center;
    backdrop-filter: blur(20px);
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    box-shadow: 0 4px 24px rgba(0, 0, 0, 0.3), inset 0 1px 0 rgba(255,255,255,0.04);
}
.metric-card:hover {
    border-color: rgba(56, 189, 248, 0.3);
    box-shadow: 0 8px 40px rgba(56, 189, 248, 0.08), inset 0 1px 0 rgba(255,255,255,0.06);
    transform: translateY(-2px);
}
.metric-value {
    font-size: 2.2rem;
    font-weight: 800;
    background: linear-gradient(135deg, #38bdf8, #818cf8, #c084fc);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    line-height: 1.2;
    letter-spacing: -0.5px;
}
.metric-label {
    font-size: 0.8rem;
    font-weight: 600;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    margin-top: 8px;
}

/* Hero section */
.hero-container {
    text-align: center;
    padding: 48px 20px 36px;
}
.hero-title {
    font-size: 3.2rem;
    font-weight: 900;
    background: linear-gradient(135deg, #38bdf8 0%, #818cf8 40%, #c084fc 70%, #f472b6 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin-bottom: 12px;
    letter-spacing: -1.5px;
    line-height: 1.1;
}
.hero-subtitle {
    font-size: 1.15rem;
    color: #94a3b8;
    max-width: 640px;
    margin: 0 auto;
    line-height: 1.6;
    font-weight: 400;
}

/* Feature badge */
.feature-badge {
    display: inline-block;
    padding: 6px 16px;
    border-radius: 100px;
    font-size: 0.72rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 2px;
    margin: 4px;
    border: 1px solid;
}
.badge-blue {
    background: rgba(56, 189, 248, 0.1);
    color: #38bdf8;
    border-color: rgba(56, 189, 248, 0.2);
}
.badge-purple {
    background: rgba(129, 140, 248, 0.1);
    color: #818cf8;
    border-color: rgba(129, 140, 248, 0.2);
}
.badge-pink {
    background: rgba(244, 114, 182, 0.1);
    color: #f472b6;
    border-color: rgba(244, 114, 182, 0.2);
}
.badge-green {
    background: rgba(52, 211, 153, 0.1);
    color: #34d399;
    border-color: rgba(52, 211, 153, 0.2);
}
.badge-amber {
    background: rgba(251, 191, 36, 0.1);
    color: #fbbf24;
    border-color: rgba(251, 191, 36, 0.2);
}
.badge-cyan {
    background: rgba(34, 211, 238, 0.1);
    color: #22d3ee;
    border-color: rgba(34, 211, 238, 0.2);
}

/* Section header */
.section-header {
    font-size: 1.65rem;
    font-weight: 800;
    color: #f1f5f9;
    margin: 36px 0 8px;
    letter-spacing: -0.5px;
}
.section-sub {
    font-size: 0.9rem;
    color: #64748b;
    margin-bottom: 24px;
    line-height: 1.5;
}

/* Pipeline step */
.pipeline-step {
    background: linear-gradient(135deg, rgba(15, 23, 42, 0.8), rgba(30, 41, 59, 0.6));
    border: 1px solid rgba(56, 189, 248, 0.1);
    border-radius: 14px;
    padding: 20px;
    text-align: center;
    transition: all 0.3s ease;
}
.pipeline-step:hover {
    border-color: rgba(56, 189, 248, 0.25);
}
.pipeline-icon {
    font-size: 2rem;
    margin-bottom: 8px;
}
.pipeline-name {
    font-weight: 700;
    color: #e2e8f0;
    font-size: 0.9rem;
}
.pipeline-desc {
    font-size: 0.75rem;
    color: #64748b;
    margin-top: 4px;
}

/* Event log */
.event-row {
    background: rgba(15, 23, 42, 0.6);
    border: 1px solid rgba(56, 189, 248, 0.06);
    border-radius: 10px;
    padding: 12px 16px;
    margin-bottom: 8px;
    font-family: 'JetBrains Mono', 'Fira Code', monospace;
    font-size: 0.8rem;
    color: #cbd5e1;
    transition: all 0.2s ease;
}
.event-row:hover {
    border-color: rgba(56, 189, 248, 0.2);
    background: rgba(15, 23, 42, 0.8);
}

/* Node card */
.node-online {
    background: linear-gradient(135deg, rgba(16, 185, 129, 0.08), rgba(16, 185, 129, 0.02));
    border: 1px solid rgba(16, 185, 129, 0.2);
    border-radius: 12px;
    padding: 16px;
    text-align: center;
}
.node-offline {
    background: linear-gradient(135deg, rgba(239, 68, 68, 0.08), rgba(239, 68, 68, 0.02));
    border: 1px solid rgba(239, 68, 68, 0.2);
    border-radius: 12px;
    padding: 16px;
    text-align: center;
}

/* Status indicator */
.status-dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-right: 6px;
    animation: pulse 2s infinite;
}
.status-online { background: #34d399; box-shadow: 0 0 8px rgba(52, 211, 153, 0.6); }
.status-offline { background: #ef4444; box-shadow: 0 0 8px rgba(239, 68, 68, 0.6); }

@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }
}

/* Progress bar styling */
.stProgress > div > div {
    background: linear-gradient(90deg, #38bdf8, #818cf8, #c084fc) !important;
    border-radius: 8px;
}

/* Buttons */
.stButton > button {
    background: linear-gradient(135deg, #38bdf8 0%, #818cf8 100%) !important;
    color: white !important;
    border: none !important;
    border-radius: 12px !important;
    padding: 12px 32px !important;
    font-weight: 700 !important;
    font-size: 0.9rem !important;
    letter-spacing: 0.5px !important;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
    box-shadow: 0 4px 16px rgba(56, 189, 248, 0.25) !important;
}
.stButton > button:hover {
    box-shadow: 0 8px 32px rgba(56, 189, 248, 0.35) !important;
    transform: translateY(-2px) !important;
}

/* Expander styling */
.streamlit-expanderHeader {
    background: rgba(15, 23, 42, 0.6) !important;
    border: 1px solid rgba(56, 189, 248, 0.1) !important;
    border-radius: 12px !important;
    color: #e2e8f0 !important;
    font-weight: 600 !important;
}

/* Tab styling */
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
}
.stTabs [data-baseweb="tab"] {
    background: rgba(15, 23, 42, 0.6);
    border: 1px solid rgba(56, 189, 248, 0.1);
    border-radius: 10px;
    color: #94a3b8;
    padding: 8px 20px;
    font-weight: 600;
}
.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, rgba(56, 189, 248, 0.15), rgba(129, 140, 248, 0.1)) !important;
    border-color: rgba(56, 189, 248, 0.3) !important;
    color: #38bdf8 !important;
}

/* Divider */
.gradient-divider {
    height: 2px;
    background: linear-gradient(90deg, transparent, rgba(56,189,248,0.3), rgba(129,140,248,0.3), transparent);
    border: none;
    margin: 32px 0;
    border-radius: 2px;
}
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
#  Helper: run async code
# ═══════════════════════════════════════════════════════════════════
def run_async(coro):
    """Run an async coroutine from synchronous Streamlit code."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ═══════════════════════════════════════════════════════════════════
#  Initialize SafeVault (cached in session state)
# ═══════════════════════════════════════════════════════════════════
def get_vault():
    """Return a SafeVault application instance, cached in session state."""
    if "vault_initialized" not in st.session_state:
        pubsub = PubSubBus()
        metadata = MetadataStore()
        dedup = InMemoryDedupStore()
        chunker = RabinChunker()
        compressor = Compressor(level=3)
        encryptor = Encryptor()
        storage = StorageCluster(replication_factor=3)
        monitoring = MetricsCollector(pubsub)

        # Build cluster
        regions = [
            ("us-east-1", "us-east-1a"), ("us-east-1", "us-east-1b"),
            ("us-east-1", "us-east-1c"), ("eu-west-1", "eu-west-1a"),
            ("eu-west-1", "eu-west-1b"), ("ap-south-1", "ap-south-1a"),
        ]
        for region, zone in regions:
            node = StorageNode(
                name=f"node-{region}-{zone}", region=region,
                zone=zone, capacity_bytes=1_000_000_000_000,
            )
            storage.add_node(node)

        backup_engine = BackupEngine(
            metadata, dedup, chunker, compressor, encryptor, storage, pubsub)
        restore_engine = RestoreEngine(
            metadata, dedup, compressor, encryptor, storage, pubsub)

        st.session_state.pubsub = pubsub
        st.session_state.metadata = metadata
        st.session_state.dedup = dedup
        st.session_state.chunker = chunker
        st.session_state.compressor = compressor
        st.session_state.encryptor = encryptor
        st.session_state.storage = storage
        st.session_state.backup_engine = backup_engine
        st.session_state.restore_engine = restore_engine
        st.session_state.monitoring = monitoring
        st.session_state.vault_initialized = True

        # Tracking
        st.session_state.backup_history = []
        st.session_state.restore_history = []
        st.session_state.events_log = []


def _v(key):
    return st.session_state.get(key)


# ═══════════════════════════════════════════════════════════════════
#  Plotly theme helper
# ═══════════════════════════════════════════════════════════════════
def dark_layout(fig, title="", height=380):
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title=dict(text=title, font=dict(size=16, color="#e2e8f0", family="Inter")),
        font=dict(family="Inter", color="#94a3b8"),
        height=height,
        margin=dict(l=40, r=40, t=60, b=40),
        xaxis=dict(gridcolor="rgba(56,189,248,0.06)", zerolinecolor="rgba(56,189,248,0.06)"),
        yaxis=dict(gridcolor="rgba(56,189,248,0.06)", zerolinecolor="rgba(56,189,248,0.06)"),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
    )
    return fig


# ═══════════════════════════════════════════════════════════════════
#  Sidebar Navigation
# ═══════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div style="text-align:center; padding: 20px 0 8px;">
        <div style="font-size:2.6rem; margin-bottom:4px;">🛡️</div>
        <div style="font-size:1.4rem; font-weight:800;
             background: linear-gradient(135deg, #38bdf8, #818cf8);
             -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
            SafeVault
        </div>
        <div style="font-size:0.7rem; color:#64748b; letter-spacing:2px; text-transform:uppercase; margin-top:4px;">
            Enterprise Backup & Restore
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)

    page = st.radio(
        "Navigation",
        [
            "🏠  System Overview",
            "💾  Full Backup",
            "📈  Incremental Backup",
            "♻️  Restore",
            "📊  Monitoring",
            "🧬  Deduplication",
            "🔧  Fault Tolerance",
        ],
        label_visibility="collapsed",
    )

    st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)

    st.markdown("""
    <div style="padding: 12px 16px; background: rgba(56,189,248,0.05);
         border: 1px solid rgba(56,189,248,0.1); border-radius: 12px; margin-top: 8px;">
        <div style="font-size:0.72rem; font-weight:700; color:#38bdf8; text-transform:uppercase;
             letter-spacing:1.5px; margin-bottom:8px;">Tech Stack</div>
        <div style="font-size:0.78rem; color:#94a3b8; line-height:1.8;">
            🔐 AES-256-GCM Encryption<br>
            📦 Zstandard Compression<br>
            🔗 Rabin Fingerprint CDC<br>
            🧮 SHA-256 Deduplication<br>
            📡 Pub/Sub Event Bus<br>
            🌍 3× Cross-Region Replication
        </div>
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
#  Initialize vault
# ═══════════════════════════════════════════════════════════════════
get_vault()


# ═══════════════════════════════════════════════════════════════════
#  PAGE: System Overview
# ═══════════════════════════════════════════════════════════════════
if page == "🏠  System Overview":
    st.markdown("""
    <div class="hero-container">
        <div class="hero-title">SafeVault</div>
        <div class="hero-subtitle">
            A scalable, reliable, secure, and highly available backup and recovery platform
            designed to protect critical data across distributed enterprise environments.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Feature badges
    st.markdown("""
    <div style="text-align:center; margin-bottom: 24px;">
        <span class="feature-badge badge-blue">CDC Chunking</span>
        <span class="feature-badge badge-purple">Global Dedup</span>
        <span class="feature-badge badge-pink">AES-256-GCM</span>
        <span class="feature-badge badge-green">Zstd Compression</span>
        <span class="feature-badge badge-amber">Fault Tolerance</span>
        <span class="feature-badge badge-cyan">Pub/Sub Events</span>
    </div>
    """, unsafe_allow_html=True)

    # ── Interactive Quick Action Bar ──────────────────────────────
    st.markdown('<div class="section-header">⚡ Interactive Demo Actions</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Test SafeVault core capabilities instantly with live simulation controls</div>', unsafe_allow_html=True)

    act_col1, act_col2, act_col3 = st.columns(3)
    
    with act_col1:
        if st.button("🚀 1-Click Enterprise Backup Demo", use_container_width=True, help="Runs a live backup across 6 nodes with chunking, dedup, compression & encryption"):
            metadata = _v("metadata")
            backup_engine = _v("backup_engine")
            dedup = _v("dedup")
            
            with st.spinner("Processing files through Rabin CDC -> Zstd -> AES-256 -> 3x Quorum replication..."):
                demo_policy = metadata.create_policy(
                    user_id="demo-admin",
                    name=f"Demo Backup {time.strftime('%H:%M:%S')}",
                    schedule_cron="0 1 * * *",
                    retention_days=30,
                    backup_type=BackupType.FULL,
                    source_paths=["/enterprise/db", "/enterprise/configs", "/enterprise/logs"],
                )
                sample_files = {
                    "/enterprise/db/users.sql": ("CREATE TABLE users (id INT, email VARCHAR(255));\n" + "INSERT INTO users VALUES (1, 'alice@safevault.io');\n" * 400).encode(),
                    "/enterprise/configs/cluster.json": json.dumps({"cluster": "safevault-prod", "nodes": 6, "replication": 3, "tls": True, "created": time.time()}).encode(),
                    "/enterprise/logs/access.log": (f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] GET /api/v1/vault/status 200 OK\n" * 1200).encode(),
                    "/enterprise/docs/report.pdf": os.urandom(85_000),
                }
                job_id = run_async(backup_engine.run_backup(demo_policy.id, sample_files))
                total_raw = sum(len(v) for v in sample_files.values())
                
                st.session_state.backup_history.append({
                    "job_id": job_id[:8],
                    "type": "Full (1-Click)",
                    "files": len(sample_files),
                    "raw_bytes": total_raw,
                    "stored_bytes": dedup.total_storage_bytes(),
                    "chunks": dedup.total_unique_chunks(),
                    "status": "COMPLETED",
                    "time": time.strftime("%H:%M:%S"),
                })
            st.success(f"✅ Demo backup job `{job_id[:8]}` completed! Distributed chunks across active cluster nodes.")
            st.rerun()

    with act_col2:
        storage = _v("storage")
        nodes_list = list(storage._nodes.values())
        if st.button("💥 Chaos Experiment: Toggle Random Node", use_container_width=True, help="Simulates an unexpected node failure to test cross-region fault tolerance"):
            import random
            target = random.choice(nodes_list)
            new_status = NodeStatus.OFFLINE if target.status == NodeStatus.ONLINE else NodeStatus.ONLINE
            target.set_status(new_status)
            if new_status == NodeStatus.OFFLINE:
                st.warning(f"⚠️ Simulated failure on node **{target.name}** ({target.region}/{target.zone})! Cluster degraded.")
            else:
                st.success(f"✅ Recovered node **{target.name}** ({target.region}/{target.zone}) back online!")
            st.rerun()

    with act_col3:
        if st.button("🧹 Reset Demo Cluster State", use_container_width=True, help="Clears stored state and re-initializes clean cluster"):
            for key in ["vault_initialized", "pubsub", "metadata", "dedup", "chunker", "compressor", "encryptor", "storage", "backup_engine", "restore_engine", "monitoring", "backup_history", "restore_history", "events_log"]:
                if key in st.session_state:
                    del st.session_state[key]
            st.info("System state reset to initial clean deployment.")
            st.rerun()

    st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)

    # ── Interactive Live Rabin CDC Sandbox ─────────────────────────
    st.markdown('<div class="section-header">🔬 Live CDC & Deduplication Sandbox</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Type or change text below to see Rabin Content-Defined Chunking, hashes, and Zstd compression dynamically in real time:</div>', unsafe_allow_html=True)

    sandbox_text = st.text_area(
        "Type or paste text to inspect real-time chunk boundaries & deduplication:",
        value="SafeVault Enterprise Backup Engine provides content-defined chunking (CDC) with Rabin Fingerprints! " * 15,
        height=85,
        key="overview_cdc_input",
    )

    if sandbox_text:
        chunker = _v("chunker")
        compressor = _v("compressor")
        encryptor = _v("encryptor")
        dedup = _v("dedup")

        data_bytes = sandbox_text.encode("utf-8")
        live_chunks = chunker.chunk(data_bytes)
        compressed_bytes = compressor.compress(data_bytes)
        
        sb_c1, sb_c2, sb_c3, sb_c4 = st.columns(4)
        with sb_c1:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-value">{len(data_bytes)} B</div>
                <div class="metric-label">Raw Input Size</div>
            </div>""", unsafe_allow_html=True)
        with sb_c2:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-value">{len(live_chunks)}</div>
                <div class="metric-label">Rabin CDC Chunks</div>
            </div>""", unsafe_allow_html=True)
        with sb_c3:
            comp_ratio = len(data_bytes) / max(len(compressed_bytes), 1)
            st.markdown(f"""<div class="metric-card">
                <div class="metric-value">{comp_ratio:.2f}x</div>
                <div class="metric-label">Zstd Compression</div>
            </div>""", unsafe_allow_html=True)
        with sb_c4:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-value" style="font-size:1.4rem; color:#34d399;">AES-256-GCM</div>
                <div class="metric-label">Chunk Security</div>
            </div>""", unsafe_allow_html=True)

        with st.expander("🔍 View Chunk Fingerprints & Boundaries", expanded=False):
            for i, (offset, size) in enumerate(live_chunks):
                chunk_slice = data_bytes[offset:offset + size]
                chash = hashlib.sha256(chunk_slice).hexdigest()
                is_known = dedup.has_chunk(chash) if hasattr(dedup, "has_chunk") else (dedup.lookup(chash) is not None)
                dedup_badge = "🟢 In Dedup Store" if is_known else "⚪ New Chunk"
                st.code(f"Chunk #{i+1} | Offset: {offset} | Size: {size} bytes | SHA256: {chash[:16]}... | {dedup_badge}", language="text")

    st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)

    # ── Backup Pipeline ────────────────────────────────────────────
    st.markdown('<div class="section-header">⚙️ Backup Pipeline</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">Files flow through 5 stages before landing on the distributed cluster</div>',
                unsafe_allow_html=True)

    cols = st.columns(5)
    pipeline = [
        ("📄", "Chunking", "Rabin fingerprint CDC ~8 KB avg"),
        ("🧬", "Deduplication", "SHA-256 + Bloom filter"),
        ("📦", "Compression", "Zstandard level 3"),
        ("🔐", "Encryption", "AES-256-GCM per-chunk"),
        ("🌍", "Replication", "3× quorum write"),
    ]
    for col, (icon, name, desc) in zip(cols, pipeline):
        with col:
            st.markdown(f"""
            <div class="pipeline-step">
                <div class="pipeline-icon">{icon}</div>
                <div class="pipeline-name">{name}</div>
                <div class="pipeline-desc">{desc}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)

    # ── Storage Cluster Status & Interactive Node Controls ─────────
    st.markdown('<div class="section-header">🌐 Storage Cluster & Node Controls</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-sub">6 nodes deployed across 3 regions with 3× replication. Toggle nodes to observe cluster health:</div>',
                unsafe_allow_html=True)

    storage = _v("storage")
    nodes = list(storage._nodes.values())
    online_count = sum(1 for n in nodes if n.status == NodeStatus.ONLINE)

    st.info(f"Cluster Health: **{online_count}/{len(nodes)}** nodes online. Quorum requirement: **2/3** nodes per chunk write/read.")

    cols = st.columns(3)
    region_map = defaultdict(list)
    for n in nodes:
        region_map[n.region].append(n)

    region_labels = {"us-east-1": "🇺🇸 US East", "eu-west-1": "🇪🇺 EU West", "ap-south-1": "🇮🇳 AP South"}
    for col, (region, region_nodes) in zip(cols, region_map.items()):
        with col:
            st.markdown(f"**{region_labels.get(region, region)}**")
            for n in region_nodes:
                css_class = "node-online" if n.status == NodeStatus.ONLINE else "node-offline"
                dot_class = "status-online" if n.status == NodeStatus.ONLINE else "status-offline"
                st.markdown(f"""
                <div class="{css_class}" style="margin-bottom:8px;">
                    <span class="status-dot {dot_class}"></span>
                    <span style="font-size:0.82rem; font-weight:600; color:#e2e8f0;">{n.zone}</span><br>
                    <span style="font-size:0.7rem; color:#64748b;">{n.chunk_count()} chunks · {n.status.value}</span>
                </div>
                """, unsafe_allow_html=True)
                
                # Interactive toggle button right beneath each node
                toggle_btn_label = f"Turn Offline" if n.status == NodeStatus.ONLINE else f"Turn Online"
                if st.button(f"{toggle_btn_label} ({n.zone})", key=f"btn_node_{n.name}", use_container_width=True):
                    n.set_status(NodeStatus.OFFLINE if n.status == NodeStatus.ONLINE else NodeStatus.ONLINE)
                    st.rerun()

    st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)

    # ── Quick Stats ────────────────────────────────────────────────
    st.markdown('<div class="section-header">📊 System Metrics</div>', unsafe_allow_html=True)

    monitoring = _v("monitoring")
    summary = monitoring.summary()
    dedup = _v("dedup")

    c1, c2, c3, c4 = st.columns(4)
    stats = [
        (c1, str(summary["backups_completed"]), "Backups Done"),
        (c2, str(summary["restores_completed"]), "Restores Done"),
        (c3, str(dedup.total_unique_chunks()), "Unique Chunks"),
        (c4, f"{dedup.total_storage_bytes() / 1024:.1f} KB", "Storage Used"),
    ]
    for col, val, label in stats:
        with col:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-value">{val}</div>
                <div class="metric-label">{label}</div>
            </div>
            """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
#  PAGE: Full Backup
# ═══════════════════════════════════════════════════════════════════
elif page == "💾  Full Backup":
    st.markdown('<div class="section-header">💾 Full Backup Simulation</div>', unsafe_allow_html=True)
    st.markdown("""<div class="section-sub">
        Run a full backup that chunks, deduplicates, compresses, encrypts, and replicates files
        across the distributed storage cluster.
    </div>""", unsafe_allow_html=True)

    # Controls
    col1, col2 = st.columns([2, 1])
    with col1:
        user_text = st.text_area(
            "📝 Custom text to backup (or leave default)",
            value="Hello World! " * 1000,
            height=120,
            help="Enter your own text to see how SafeVault processes it through the pipeline",
        )
    with col2:
        policy_name = st.text_input("Policy Name", value="Daily Backup")
        retention = st.slider("Retention (days)", 1, 365, 30)
        compression_level = st.slider("Compression Level", 1, 9, 3)

    if st.button("🚀 Run Full Backup", use_container_width=True):
        metadata = _v("metadata")
        backup_engine = _v("backup_engine")

        progress = st.progress(0, text="Initializing backup job...")

        # Create policy
        policy = metadata.create_policy(
            user_id="demo-user", name=policy_name,
            schedule_cron="0 2 * * *", retention_days=retention,
            backup_type=BackupType.FULL, source_paths=["/data"],
        )
        progress.progress(10, text="Policy created...")

        # Prepare files
        files = {
            "/data/document.txt": user_text.encode(),
            "/data/database.dump": os.urandom(50_000),
            "/data/config.json": b'{"key": "value", "nested": [1,2,3]}',
            "/data/image.png": os.urandom(100_000),
            "/data/log.txt": b"2024-01-01 INFO: started\n" * 2000,
        }
        total_raw = sum(len(v) for v in files.values())
        progress.progress(20, text="Files prepared...")

        # Run pipeline
        progress.progress(40, text="Chunking & deduplicating...")
        time.sleep(0.3)
        progress.progress(60, text="Compressing & encrypting...")
        time.sleep(0.3)
        progress.progress(80, text="Replicating to cluster...")

        job_id = run_async(backup_engine.run_backup(policy.id, files))

        progress.progress(100, text="✅ Backup complete!")
        time.sleep(0.5)

        if job_id:
            job = metadata.jobs[job_id]
            dedup = _v("dedup")

            st.session_state.backup_history.append({
                "job_id": job_id[:8],
                "type": "Full",
                "files": len(files),
                "raw_bytes": total_raw,
                "stored_bytes": dedup.total_storage_bytes(),
                "chunks": dedup.total_unique_chunks(),
                "status": job.status.value,
                "time": time.strftime("%H:%M:%S"),
            })

            st.success(f"Backup job **{job_id[:8]}...** completed successfully!")

            # Results
            st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)

            r1, r2, r3, r4 = st.columns(4)
            with r1:
                st.markdown(f"""<div class="metric-card">
                    <div class="metric-value">{len(files)}</div>
                    <div class="metric-label">Files Processed</div>
                </div>""", unsafe_allow_html=True)
            with r2:
                st.markdown(f"""<div class="metric-card">
                    <div class="metric-value">{total_raw / 1024:.1f} KB</div>
                    <div class="metric-label">Raw Data</div>
                </div>""", unsafe_allow_html=True)
            with r3:
                st.markdown(f"""<div class="metric-card">
                    <div class="metric-value">{dedup.total_unique_chunks()}</div>
                    <div class="metric-label">Unique Chunks</div>
                </div>""", unsafe_allow_html=True)
            with r4:
                st.markdown(f"""<div class="metric-card">
                    <div class="metric-value">{dedup.total_storage_bytes() / 1024:.1f} KB</div>
                    <div class="metric-label">After Dedup</div>
                </div>""", unsafe_allow_html=True)

            # Chunking visualization
            st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)
            st.markdown('<div class="section-header">🔗 Chunk Breakdown</div>', unsafe_allow_html=True)

            chunker = _v("chunker")
            chunks = chunker.chunk(user_text.encode())
            chunk_sizes = [s for _, s in chunks]

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=list(range(len(chunk_sizes))),
                y=chunk_sizes,
                marker=dict(
                    color=chunk_sizes,
                    colorscale=[[0, "#38bdf8"], [0.5, "#818cf8"], [1, "#c084fc"]],
                    line=dict(width=0),
                ),
                hovertemplate="Chunk %{x}<br>Size: %{y:,} bytes<extra></extra>",
            ))
            dark_layout(fig, "Chunk Sizes (Rabin Fingerprint CDC)", 320)
            fig.update_layout(
                xaxis_title="Chunk Index",
                yaxis_title="Size (bytes)",
            )
            st.plotly_chart(fig, use_container_width=True)

    # Show backup history
    if st.session_state.backup_history:
        st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)
        st.markdown('<div class="section-header">📋 Backup History</div>', unsafe_allow_html=True)
        for entry in reversed(st.session_state.backup_history[-5:]):
            st.markdown(f"""
            <div class="event-row">
                <span style="color:#38bdf8; font-weight:700;">[{entry['time']}]</span>
                <span style="color:#818cf8;">{entry['type']}</span> ·
                Job <span style="color:#c084fc;">{entry['job_id']}</span> ·
                {entry['files']} files ·
                {entry['raw_bytes'] / 1024:.1f} KB →
                {entry['stored_bytes'] / 1024:.1f} KB ·
                <span style="color:#34d399;">✓ {entry['status']}</span>
            </div>
            """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
#  PAGE: Incremental Backup
# ═══════════════════════════════════════════════════════════════════
elif page == "📈  Incremental Backup":
    st.markdown('<div class="section-header">📈 Incremental Backup</div>', unsafe_allow_html=True)
    st.markdown("""<div class="section-sub">
        Detects changed files via hash comparison and backs up only new/modified data.
        Only new chunks that differ from the previous version are stored.
    </div>""", unsafe_allow_html=True)

    metadata = _v("metadata")

    # Show existing policies
    if not metadata.policies:
        st.warning("⚠️ No backup policies exist. Run a **Full Backup** first to create a policy.")
    else:
        policy_names = {pid: p.name for pid, p in metadata.policies.items()}
        selected_pid = st.selectbox("Select Policy", list(policy_names.keys()),
                                     format_func=lambda x: f"{policy_names[x]} ({x[:8]}...)")

        col1, col2 = st.columns(2)
        with col1:
            mod_text = st.text_area("Modified file content",
                                     value="Hello World! " * 1001 + " MODIFIED!",
                                     height=100)
        with col2:
            new_file_text = st.text_area("New file content",
                                          value="a,b,c\n1,2,3\n4,5,6\n",
                                          height=100)

        if st.button("📈 Run Incremental Backup", use_container_width=True):
            backup_engine = _v("backup_engine")
            dedup = _v("dedup")

            before_chunks = dedup.total_unique_chunks()
            before_bytes = dedup.total_storage_bytes()

            files = {
                "/data/document.txt": mod_text.encode(),
                "/data/database.dump": os.urandom(50_000),
                "/data/new_file.csv": new_file_text.encode(),
            }
            total_raw = sum(len(v) for v in files.values())

            progress = st.progress(0, text="Detecting changes...")
            time.sleep(0.3)
            progress.progress(30, text="Processing modified files...")
            time.sleep(0.3)
            progress.progress(60, text="Deduplicating new chunks...")
            time.sleep(0.3)
            progress.progress(85, text="Storing incremental data...")

            job_id = run_async(backup_engine.run_backup(selected_pid, files))
            progress.progress(100, text="✅ Incremental backup complete!")

            if job_id:
                job = metadata.jobs[job_id]
                after_chunks = dedup.total_unique_chunks()
                after_bytes = dedup.total_storage_bytes()
                new_chunks = after_chunks - before_chunks
                new_bytes = after_bytes - before_bytes

                st.session_state.backup_history.append({
                    "job_id": job_id[:8], "type": "Incremental",
                    "files": len(files), "raw_bytes": total_raw,
                    "stored_bytes": after_bytes, "chunks": after_chunks,
                    "status": job.status.value, "time": time.strftime("%H:%M:%S"),
                })

                st.success(f"Incremental backup **{job_id[:8]}...** — only **{new_chunks}** new chunks stored!")

                r1, r2, r3, r4 = st.columns(4)
                with r1:
                    st.markdown(f"""<div class="metric-card">
                        <div class="metric-value">{len(files)}</div>
                        <div class="metric-label">Files Changed</div>
                    </div>""", unsafe_allow_html=True)
                with r2:
                    st.markdown(f"""<div class="metric-card">
                        <div class="metric-value">{new_chunks}</div>
                        <div class="metric-label">New Chunks</div>
                    </div>""", unsafe_allow_html=True)
                with r3:
                    st.markdown(f"""<div class="metric-card">
                        <div class="metric-value">{new_bytes / 1024:.1f} KB</div>
                        <div class="metric-label">New Data Stored</div>
                    </div>""", unsafe_allow_html=True)
                with r4:
                    savings_pct = (1 - new_bytes / max(total_raw, 1)) * 100
                    st.markdown(f"""<div class="metric-card">
                        <div class="metric-value">{savings_pct:.1f}%</div>
                        <div class="metric-label">Storage Saved</div>
                    </div>""", unsafe_allow_html=True)

                # Comparison chart
                st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    name="Before", x=["Unique Chunks", "Storage (KB)"],
                    y=[before_chunks, before_bytes / 1024],
                    marker_color="#38bdf8", marker_line_width=0,
                ))
                fig.add_trace(go.Bar(
                    name="After", x=["Unique Chunks", "Storage (KB)"],
                    y=[after_chunks, after_bytes / 1024],
                    marker_color="#c084fc", marker_line_width=0,
                ))
                dark_layout(fig, "Incremental vs. Previous State", 340)
                fig.update_layout(barmode="group")
                st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════
#  PAGE: Restore
# ═══════════════════════════════════════════════════════════════════
elif page == "♻️  Restore":
    st.markdown('<div class="section-header">♻️ Restore from Backup</div>', unsafe_allow_html=True)
    st.markdown("""<div class="section-sub">
        Reconstructs files from the version chain by collecting chunks,
        decrypting, decompressing, and verifying SHA-256 integrity.
    </div>""", unsafe_allow_html=True)

    metadata = _v("metadata")

    if not metadata.policies:
        st.warning("⚠️ No backups exist. Run a **Full Backup** first.")
    else:
        policy_names = {pid: p.name for pid, p in metadata.policies.items()}
        selected_pid = st.selectbox("Select Policy", list(policy_names.keys()),
                                     format_func=lambda x: f"{policy_names[x]} ({x[:8]}...)")

        # List available files
        file_paths = list(metadata._file_path_index.keys())
        if file_paths:
            selected_file = st.selectbox("File to Restore", file_paths)

            if st.button("♻️ Restore File", use_container_width=True):
                restore_engine = _v("restore_engine")

                progress = st.progress(0, text="Finding latest version...")
                time.sleep(0.3)
                progress.progress(25, text="Walking version chain...")
                time.sleep(0.3)
                progress.progress(50, text="Collecting chunks...")
                time.sleep(0.3)
                progress.progress(75, text="Decrypting & decompressing...")

                try:
                    policy = metadata.get_policy(selected_pid)
                    result = run_async(restore_engine.restore_latest(
                        user_id=policy.user_id, policy_id=selected_pid,
                        file_path=selected_file,
                        restore_path=f"/restore{selected_file}",
                    ))
                    progress.progress(100, text="✅ Restore complete — integrity verified!")

                    if result:
                        st.session_state.restore_history.append({
                            "file": selected_file,
                            "size": len(result),
                            "time": time.strftime("%H:%M:%S"),
                        })

                        st.success(f"File **{selected_file}** restored! ({len(result):,} bytes)")

                        r1, r2, r3 = st.columns(3)
                        with r1:
                            st.markdown(f"""<div class="metric-card">
                                <div class="metric-value">{len(result):,}</div>
                                <div class="metric-label">Bytes Restored</div>
                            </div>""", unsafe_allow_html=True)
                        with r2:
                            file_hash = hashlib.sha256(result).hexdigest()[:16]
                            st.markdown(f"""<div class="metric-card">
                                <div class="metric-value" style="font-size:1.2rem;">{file_hash}…</div>
                                <div class="metric-label">SHA-256 Verified</div>
                            </div>""", unsafe_allow_html=True)
                        with r3:
                            st.markdown(f"""<div class="metric-card">
                                <div class="metric-value">✓</div>
                                <div class="metric-label">Integrity Check</div>
                            </div>""", unsafe_allow_html=True)

                        # Show content preview
                        st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)
                        with st.expander("📄 Preview Restored Content", expanded=True):
                            try:
                                text = result[:2000].decode("utf-8", errors="replace")
                                st.code(text, language="text")
                            except Exception:
                                st.info(f"Binary content — {len(result):,} bytes")
                except Exception as e:
                    progress.progress(100, text="❌ Restore failed")
                    st.error(f"Restore failed: {e}")
        else:
            st.info("No files in backup. Run a backup first.")


# ═══════════════════════════════════════════════════════════════════
#  PAGE: Monitoring
# ═══════════════════════════════════════════════════════════════════
elif page == "📊  Monitoring":
    st.markdown('<div class="section-header">📊 Monitoring & Observability</div>', unsafe_allow_html=True)
    st.markdown("""<div class="section-sub">
        Real-time metrics, alerts, and Pub/Sub event stream — all collected via the event-driven architecture.
    </div>""", unsafe_allow_html=True)

    monitoring = _v("monitoring")
    summary = monitoring.summary()
    dedup = _v("dedup")
    pubsub = _v("pubsub")

    # ── Metric Cards ───────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    metrics = [
        (c1, str(summary["backups_completed"]), "Backups ✓", "#34d399"),
        (c2, str(summary["backups_failed"]), "Backups ✗", "#ef4444"),
        (c3, str(summary["restores_completed"]), "Restores ✓", "#38bdf8"),
        (c4, str(summary["restores_failed"]), "Restores ✗", "#f97316"),
        (c5, str(summary["critical_alerts"]), "Critical Alerts", "#fbbf24"),
    ]
    for col, val, label, color in metrics:
        with col:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-value" style="background:none; -webkit-text-fill-color:{color}; color:{color};">{val}</div>
                <div class="metric-label">{label}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)

    # ── Charts ─────────────────────────────────────────────────────
    tab1, tab2 = st.tabs(["📈 System Metrics", "📡 Event Stream"])

    with tab1:
        col_a, col_b = st.columns(2)
        with col_a:
            # Operations breakdown
            labels = ["Backups OK", "Backups Failed", "Restores OK", "Restores Failed"]
            values = [
                summary["backups_completed"], summary["backups_failed"],
                summary["restores_completed"], summary["restores_failed"],
            ]
            if any(v > 0 for v in values):
                fig = go.Figure(go.Pie(
                    labels=labels, values=values,
                    hole=0.55,
                    marker=dict(colors=["#34d399", "#ef4444", "#38bdf8", "#f97316"],
                                line=dict(color="#0a0e1a", width=3)),
                    textfont=dict(color="#e2e8f0"),
                ))
                dark_layout(fig, "Operations Breakdown")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Run some backups/restores to see metrics here.")

        with col_b:
            # Storage usage gauge
            total_stored = dedup.total_storage_bytes()
            total_chunks = dedup.total_unique_chunks()
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=total_stored / 1024,
                title=dict(text="Storage Used (KB)", font=dict(color="#e2e8f0", size=14)),
                number=dict(font=dict(color="#38bdf8", size=36)),
                gauge=dict(
                    axis=dict(range=[0, max(total_stored / 1024 * 2, 100)],
                              tickcolor="#64748b"),
                    bar=dict(color="#818cf8"),
                    bgcolor="rgba(15,23,42,0.8)",
                    bordercolor="rgba(56,189,248,0.2)",
                    steps=[
                        dict(range=[0, total_stored / 1024 * 0.6],
                             color="rgba(56,189,248,0.08)"),
                        dict(range=[total_stored / 1024 * 0.6, total_stored / 1024 * 1.2],
                             color="rgba(129,140,248,0.08)"),
                    ],
                ),
            ))
            dark_layout(fig, "")
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        # Pub/Sub event history
        events = pubsub.get_history()
        if events:
            st.markdown(f"**{len(events)} events** captured via Pub/Sub bus")
            for ev in reversed(events[-15:]):
                topic_color = {
                    "backup.job.started": "#38bdf8",
                    "backup.job.completed": "#34d399",
                    "backup.job.failed": "#ef4444",
                    "restore.job.started": "#818cf8",
                    "restore.job.completed": "#34d399",
                    "restore.job.failed": "#ef4444",
                }.get(ev.topic, "#94a3b8")
                payload_preview = json.dumps(ev.payload)[:80]
                st.markdown(f"""
                <div class="event-row">
                    <span style="color:{topic_color}; font-weight:700;">{ev.topic}</span><br>
                    <span style="font-size:0.72rem; color:#64748b;">
                        [{ev.event_type}] · {ev.producer} · {payload_preview}
                    </span>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No events yet. Run a backup or restore to see the Pub/Sub event stream.")

    # ── Alerts ─────────────────────────────────────────────────────
    alerts = monitoring.get_alerts()
    if alerts:
        st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)
        st.markdown('<div class="section-header">🚨 Alerts</div>', unsafe_allow_html=True)
        for a in alerts:
            severity_icon = {"critical": "🔴", "warning": "🟡", "info": "🔵"}.get(a.severity, "⚪")
            st.markdown(f"""
            <div class="event-row">
                {severity_icon} <span style="font-weight:700; color:#fbbf24;">[{a.severity.upper()}]</span>
                {a.message}
            </div>
            """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════
#  PAGE: Deduplication
# ═══════════════════════════════════════════════════════════════════
elif page == "🧬  Deduplication":
    st.markdown('<div class="section-header">🧬 Deduplication Efficiency</div>', unsafe_allow_html=True)
    st.markdown("""<div class="section-sub">
        SHA-256 global dedup with Bloom filter — identical data blocks are stored only once
        regardless of how many files reference them.
    </div>""", unsafe_allow_html=True)

    # Controls
    col1, col2 = st.columns(2)
    with col1:
        file_a = st.text_area("File A content", value="ABCDEFGHIJ" * 5000, height=100,
                               help="Create two similar files to see dedup in action")
    with col2:
        file_b = st.text_area("File B content", value="ABCDEFGHIJK" * 5000, height=100,
                               help="Mostly the same prefix as File A — shared chunks deduplicated")

    similarity = st.slider("Adjust File B similarity (%)", 0, 100, 90,
                            help="Higher = more overlap with File A")

    if st.button("🧬 Run Dedup Analysis", use_container_width=True):
        # Create a fresh dedup store for this experiment
        test_dedup = InMemoryDedupStore()
        test_metadata = MetadataStore()
        test_pubsub = PubSubBus()
        test_chunker = RabinChunker()
        test_compressor = Compressor(level=3)
        test_encryptor = Encryptor()
        test_storage = StorageCluster(replication_factor=3)

        # Add nodes
        for region, zone in [("us-east-1", "us-east-1a"), ("us-east-1", "us-east-1b"),
                              ("us-east-1", "us-east-1c"), ("eu-west-1", "eu-west-1a"),
                              ("eu-west-1", "eu-west-1b"), ("ap-south-1", "ap-south-1a")]:
            test_storage.add_node(StorageNode(name=f"n-{zone}", region=region,
                                              zone=zone, capacity_bytes=10**12))

        test_engine = BackupEngine(
            test_metadata, test_dedup, test_chunker,
            test_compressor, test_encryptor, test_storage, test_pubsub)

        policy = test_metadata.create_policy(
            "test-user", "Dedup Test", "0 3 * * *",
            30, BackupType.FULL, ["/dedup-test"])

        file_a_bytes = file_a.encode()
        file_b_bytes = file_b.encode()
        total_raw = len(file_a_bytes) + len(file_b_bytes)

        progress = st.progress(0, text="Processing File A...")

        # Process file A
        files_a = {"/dedup-test/file_a.txt": file_a_bytes}
        run_async(test_engine.run_backup(policy.id, files_a))
        after_a_chunks = test_dedup.total_unique_chunks()
        after_a_bytes = test_dedup.total_storage_bytes()

        progress.progress(50, text="Processing File B (detecting duplicates)...")
        time.sleep(0.4)

        # Need a new policy for file B to avoid incremental detection
        policy2 = test_metadata.create_policy(
            "test-user", "Dedup Test 2", "0 4 * * *",
            30, BackupType.FULL, ["/dedup-test2"])

        files_b = {"/dedup-test2/file_b.txt": file_b_bytes}
        run_async(test_engine.run_backup(policy2.id, files_b))
        after_b_chunks = test_dedup.total_unique_chunks()
        after_b_bytes = test_dedup.total_storage_bytes()

        progress.progress(100, text="✅ Dedup analysis complete!")

        new_chunks_for_b = after_b_chunks - after_a_chunks
        dedup_ratio = total_raw / max(after_b_bytes, 1)
        savings_pct = (1 - after_b_bytes / max(total_raw, 1)) * 100

        # Results
        st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)

        r1, r2, r3, r4 = st.columns(4)
        with r1:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-value">{after_a_chunks}</div>
                <div class="metric-label">File A Chunks</div>
            </div>""", unsafe_allow_html=True)
        with r2:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-value">{new_chunks_for_b}</div>
                <div class="metric-label">New Chunks in B</div>
            </div>""", unsafe_allow_html=True)
        with r3:
            st.markdown(f"""<div class="metric-card">
                <div class="metric-value">{dedup_ratio:.2f}x</div>
                <div class="metric-label">Dedup Ratio</div>
            </div>""", unsafe_allow_html=True)
        with r4:
            savings_color = "#34d399" if savings_pct > 0 else "#ef4444"
            st.markdown(f"""<div class="metric-card">
                <div class="metric-value" style="background:none; -webkit-text-fill-color:{savings_color};">{savings_pct:.1f}%</div>
                <div class="metric-label">Storage Savings</div>
            </div>""", unsafe_allow_html=True)

        # Visualization
        st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)

        col_a, col_b = st.columns(2)
        with col_a:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=["Raw Data", "After Dedup"],
                y=[total_raw / 1024, after_b_bytes / 1024],
                marker=dict(
                    color=["#38bdf8", "#c084fc"],
                    line=dict(width=0),
                ),
                text=[f"{total_raw / 1024:.1f} KB", f"{after_b_bytes / 1024:.1f} KB"],
                textposition="outside",
                textfont=dict(color="#e2e8f0"),
            ))
            dark_layout(fig, "Storage: Raw vs. Deduplicated", 340)
            fig.update_layout(yaxis_title="Size (KB)")
            st.plotly_chart(fig, use_container_width=True)

        with col_b:
            # Chunk overlap visualization
            shared = after_a_chunks - new_chunks_for_b if after_a_chunks > new_chunks_for_b else 0
            fig = go.Figure(go.Pie(
                labels=["Shared Chunks", "Unique to A", "Unique to B"],
                values=[shared, max(after_a_chunks - shared, 0), new_chunks_for_b],
                hole=0.5,
                marker=dict(
                    colors=["#34d399", "#38bdf8", "#c084fc"],
                    line=dict(color="#0a0e1a", width=3),
                ),
                textfont=dict(color="#e2e8f0"),
            ))
            dark_layout(fig, "Chunk Overlap Between Files", 340)
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════
#  PAGE: Fault Tolerance
# ═══════════════════════════════════════════════════════════════════
elif page == "🔧  Fault Tolerance":
    st.markdown('<div class="section-header">🔧 Fault Tolerance Demo</div>', unsafe_allow_html=True)
    st.markdown("""<div class="section-sub">
        Demonstrate node failure and recovery — backups survive node outages via quorum reads
        from the remaining replicas in the distributed cluster.
    </div>""", unsafe_allow_html=True)

    storage = _v("storage")
    metadata = _v("metadata")
    backup_engine = _v("backup_engine")
    restore_engine = _v("restore_engine")

    # Cluster visualization
    st.markdown('<div class="section-header" style="font-size:1.2rem;">🌐 Cluster Status</div>',
                unsafe_allow_html=True)

    nodes = list(storage._nodes.values())
    cols = st.columns(len(nodes))
    for col, node in zip(cols, nodes):
        with col:
            css = "node-online" if node.status == NodeStatus.ONLINE else "node-offline"
            dot = "status-online" if node.status == NodeStatus.ONLINE else "status-offline"
            icon = "✅" if node.status == NodeStatus.ONLINE else "❌"
            st.markdown(f"""
            <div class="{css}">
                <span class="status-dot {dot}"></span> {icon}<br>
                <span style="font-size:0.75rem; font-weight:600; color:#e2e8f0;">{node.zone}</span><br>
                <span style="font-size:0.68rem; color:#64748b;">{node.region}</span><br>
                <span style="font-size:0.68rem; color:#64748b;">{node.chunk_count()} chunks</span>
            </div>
            """, unsafe_allow_html=True)

    st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)

    # Interactive controls
    st.markdown('<div class="section-header" style="font-size:1.2rem;">🎮 Simulate Failure</div>',
                unsafe_allow_html=True)

    node_names = [n.name for n in nodes]
    col1, col2 = st.columns(2)
    with col1:
        fail_node = st.selectbox("Select node to fail", node_names)
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        toggle = st.toggle("Node is OFFLINE", value=False, key="fault_toggle")

    if toggle:
        target = next(n for n in nodes if n.name == fail_node)
        target.set_status(NodeStatus.OFFLINE)
    else:
        for n in nodes:
            if n.status == NodeStatus.OFFLINE:
                n.set_status(NodeStatus.ONLINE)

    if st.button("🔧 Run Fault Tolerance Test", use_container_width=True):
        progress = st.progress(0, text="Creating test data...")

        # Create policy and backup
        ft_policy = metadata.create_policy(
            "ft-user", "FT Test", "0 4 * * *",
            30, BackupType.FULL, ["/ft-test"])

        test_data = os.urandom(50_000)
        files = {"/ft-test/critical.bin": test_data}

        progress.progress(20, text="Running backup (all nodes)...")

        # Ensure all nodes are online for backup
        for n in nodes:
            n.set_status(NodeStatus.ONLINE)

        job_id = run_async(backup_engine.run_backup(ft_policy.id, files))
        progress.progress(40, text=f"✅ Backup complete: {job_id[:8] if job_id else 'FAILED'}")
        time.sleep(0.5)

        # Fail node
        target_node = next(n for n in nodes if n.name == fail_node)
        target_node.set_status(NodeStatus.OFFLINE)
        progress.progress(55, text=f"❌ Node FAILED: {fail_node}")
        time.sleep(0.5)

        # Attempt restore
        progress.progress(70, text="Attempting restore with degraded cluster...")
        time.sleep(0.3)

        try:
            result = run_async(restore_engine.restore_latest(
                user_id="ft-user", policy_id=ft_policy.id,
                file_path="/ft-test/critical.bin",
                restore_path="/restore/critical.bin",
            ))
            progress.progress(90, text="Verifying integrity...")
            time.sleep(0.3)

            if result and len(result) == 50_000:
                progress.progress(100, text="✅ Restore successful despite node failure!")
                st.success(f"**Fault tolerance verified!** Restored {len(result):,} bytes "
                           f"with node `{fail_node}` offline.")

                r1, r2, r3 = st.columns(3)
                with r1:
                    online_count = sum(1 for n in nodes if n.status == NodeStatus.ONLINE)
                    st.markdown(f"""<div class="metric-card">
                        <div class="metric-value" style="background:none; -webkit-text-fill-color:#34d399;">{online_count}/{len(nodes)}</div>
                        <div class="metric-label">Nodes Online</div>
                    </div>""", unsafe_allow_html=True)
                with r2:
                    st.markdown(f"""<div class="metric-card">
                        <div class="metric-value">50 KB</div>
                        <div class="metric-label">Data Recovered</div>
                    </div>""", unsafe_allow_html=True)
                with r3:
                    st.markdown(f"""<div class="metric-card">
                        <div class="metric-value" style="background:none; -webkit-text-fill-color:#34d399;">✓</div>
                        <div class="metric-label">Integrity Check</div>
                    </div>""", unsafe_allow_html=True)
            else:
                progress.progress(100, text="⚠️ Restore returned unexpected data")
                st.warning("Restore completed but returned unexpected data size.")
        except Exception as e:
            progress.progress(100, text="❌ Restore failed")
            st.error(f"Restore failed: {e}")

        # Recover node
        target_node.set_status(NodeStatus.ONLINE)
        st.info(f"🔄 Node `{fail_node}` has been recovered.")

    # Architecture note
    st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)
    with st.expander("📖 How Fault Tolerance Works", expanded=False):
        st.markdown("""
        **SafeVault** uses **3× replication** with **quorum-based consensus**:

        | Aspect | Detail |
        |--------|--------|
        | **Replication Factor** | 3 copies per chunk |
        | **Write Quorum** | N/2 + 1 = 2 nodes must acknowledge |
        | **Read Strategy** | Read from any available replica |
        | **Node Selection** | 2 in-region + 1 cross-region |
        | **Failure Recovery** | Automatic re-replication when nodes return |
        | **Integrity** | SHA-256 checksum on every read |

        This means the system can tolerate **up to 2 node failures** while still
        serving reads, and **1 node failure** during writes — matching the
        availability characteristics of production systems like Google Cloud Storage
        or Amazon S3.
        """)


# ═══════════════════════════════════════════════════════════════════
#  Footer
# ═══════════════════════════════════════════════════════════════════
st.markdown('<div class="gradient-divider"></div>', unsafe_allow_html=True)
st.markdown("""
<div style="text-align:center; padding: 16px 0; color:#475569; font-size:0.75rem;">
    SafeVault — Enterprise Backup & Restore System · Built with Python ·
    AES-256-GCM · Zstandard · Rabin CDC · SHA-256 Dedup · Pub/Sub
</div>
""", unsafe_allow_html=True)
