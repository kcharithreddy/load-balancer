#!/usr/bin/env python3
"""
embed_user_screenshots.py
Embeds the exact PNG files uploaded by the user into the Word document report.
"""
import os
from docx import Document
from docx.shared import Pt, RGBColor, Cm, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH

DOCX_PATH = "/home/charithreddy/Downloads/GRP-CHAT/report/Load_Balancer_Report_12341040.docx"
SS_DIR = "/home/charithreddy/Downloads/GRP-CHAT/report/screenshots"

# Ensure report script is re-run with exact images
doc = Document(DOCX_PATH)

# Remove old Section 9 content
sec9_idx = None
for i, p in enumerate(doc.paragraphs):
    if "9. Execution Screenshots" in p.text or "9. Screenshots" in p.text:
        sec9_idx = i
        break

if sec9_idx is not None:
    body = doc.element.body
    sec9_xml = doc.paragraphs[sec9_idx]._element
    found = False
    to_remove = []
    for elem in list(body):
        if elem is sec9_xml:
            found = True
            continue
        if found:
            to_remove.append(elem)
    for elem in to_remove:
        body.remove(elem)

# Add User Screenshots
user_screenshots = [
    ("01_lb_running_sys1.png",            "1. Load Balancer process running on Sys1 (Port 3245)"),
    ("02_backends_running.png",           "2. Flask Backends running on Sys2 (3246), Sys3 (3247), Sys4 (3248)"),
    ("03_health_endpoint.png",            "3. GET /health Endpoint Output (All backends alive)"),
    ("04_metrics_endpoint.png",           "4. GET /metrics Endpoint Output"),
    ("05_scenario_A_user_results.png",    "5. Scenario A Results — Single Backend Sys2 (User Screenshot)"),
    ("06_scenario_B_user_results.png",    "6. Scenario B Results — All Three Backends (User Screenshot)"),
    ("07_comparison_table_user.png",      "7. Side-by-Side Comparison Table (User Screenshot)"),
]

for fname, caption in user_screenshots:
    fpath = os.path.join(SS_DIR, fname)
    if os.path.exists(fpath):
        cp = doc.add_paragraph()
        cp.paragraph_format.space_before = Pt(12)
        cp.paragraph_format.space_after  = Pt(4)
        r = cp.add_run(caption)
        r.font.name = "Calibri"; r.font.size = Pt(10); r.font.bold = True
        r.font.color.rgb = RGBColor(0x1F, 0x49, 0x7D)

        ip = doc.add_paragraph()
        ip.alignment = WD_ALIGN_PARAGRAPH.LEFT
        ip.paragraph_format.space_after = Pt(12)
        run = ip.add_run()
        run.add_picture(fpath, width=Inches(6.2))

doc.save(DOCX_PATH)
print(f"✅ Report updated with user's exact uploaded screenshots!")
print(f"   Saved to: {DOCX_PATH}")
