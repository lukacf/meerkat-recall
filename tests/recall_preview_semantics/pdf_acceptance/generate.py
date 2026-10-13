#!/usr/bin/env python3
"""Original synthetic PDF fixtures; Python stdlib only, no parser or network.

--write materializes reviewed source inputs and their byte manifest.
--check compares existing files; it never derives expected text from extraction.
Expected passages are separately authored in expected.json.
SPDX-License-Identifier: MIT OR Apache-2.0
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FONT = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>"


class Pdf:
    def __init__(self):
        self.objects: list[bytes] = [b"", b""]

    def add(self, data: bytes | str) -> int:
        self.objects.append(data.encode("ascii") if isinstance(data, str) else data)
        return len(self.objects)

    def stream(self, body: bytes | str, fields: str = "") -> int:
        body = body.encode("ascii") if isinstance(body, str) else body
        return self.add(f"<< {fields} /Length {len(body)} >>\nstream\n".encode()
                        + body + b"\nendstream")

    def finish(self, pages: list[int], *, pages_extra: str = "", catalog_extra: str = "",
               info: int | None = None) -> bytes:
        self.objects[0] = f"<< /Type /Catalog /Pages 2 0 R {catalog_extra} >>".encode()
        kids = " ".join(f"{page} 0 R" for page in pages)
        self.objects[1] = f"<< /Type /Pages /Kids [{kids}] /Count {len(pages)} {pages_extra} >>".encode()
        body = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
        offsets = []
        for index, obj in enumerate(self.objects, 1):
            offsets.append(len(body))
            body.extend(f"{index} 0 obj\n".encode() + obj + b"\nendobj\n")
        start = len(body)
        body.extend(f"xref\n0 {len(offsets) + 1}\n0000000000 65535 f \n".encode())
        for offset in offsets:
            body.extend(f"{offset:010} 00000 n \n".encode())
        extra = "" if info is None else f" /Info {info} 0 R"
        body.extend(f"trailer\n<< /Size {len(offsets) + 1} /Root 1 0 R{extra} >>\n"
                    f"startxref\n{start}\n%%EOF\n".encode())
        return bytes(body)


def text_at(text: str, y: int, *, invisible: bool = False) -> str:
    value = text.encode("cp1252").hex().upper()
    return f"BT /F1 12 Tf {3 if invisible else 0} Tr 1 0 0 1 72 {y} Tm <{value}> Tj ET"


def page(pdf: Pdf, content: int, resources: str, extra: str = "") -> int:
    return pdf.add(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                   f"/Resources << {resources} >> /Contents {content} 0 R {extra} >>")


def plain_pages(streams: list[str], extra: str = "") -> bytes:
    pdf = Pdf()
    font = pdf.add(FONT)
    pages = [page(pdf, pdf.stream(stream), f"/Font << /F1 {font} 0 R >>", extra)
             for stream in streams]
    return pdf.finish(pages)


def multilingual() -> bytes:
    # Text encoding is explicit; no downloaded or embedded font program is used.
    # These fixtures qualify extraction mappings, not glyph rendering/shaping.
    lines = ["Αθήνα 2026", "Москва 2026", "東京 図書館"]
    codes = {char: index for index, char in enumerate(sorted(set("".join(lines))), 1)}
    mappings = "\n".join(f"<{code:04X}> <{char.encode('utf-16-be').hex().upper()}>"
                         for char, code in codes.items())
    cmap = ("/CIDInit /ProcSet findresource begin\n12 dict begin\nbegincmap\n"
            "/CIDSystemInfo << /Registry (Adobe) /Ordering (UCS) /Supplement 0 >> def\n"
            "/CMapName /RecallFixture def\n/CMapType 2 def\n"
            "1 begincodespacerange\n<0000> <FFFF>\nendcodespacerange\n"
            f"{len(codes)} beginbfchar\n{mappings}\nendbfchar\n"
            "endcmap\nCMapName currentdict /CMap defineresource pop\nend\nend")
    pdf = Pdf()
    cmap_id = pdf.stream(cmap)
    descendant = pdf.add("<< /Type /Font /Subtype /CIDFontType2 /BaseFont /RecallFixture "
                         "/CIDSystemInfo << /Registry (Adobe) /Ordering (Identity) /Supplement 0 >> "
                         "/DW 600 /CIDToGIDMap /Identity >>")
    font = pdf.add(f"<< /Type /Font /Subtype /Type0 /BaseFont /RecallFixture /Encoding /Identity-H "
                   f"/DescendantFonts [{descendant} 0 R] /ToUnicode {cmap_id} 0 R >>")
    content = "\n".join(f"BT /F1 12 Tf 1 0 0 1 72 {720 - index * 40} Tm "
                        f"<{''.join(f'{codes[char]:04X}' for char in line)}> Tj ET"
                        for index, line in enumerate(lines))
    return pdf.finish([page(pdf, pdf.stream(content), f"/Font << /F1 {font} 0 R >>")])


def forms(*, inherited: bool = False) -> bytes:
    pdf = Pdf()
    font = pdf.add(FONT)
    inner = pdf.stream(text_at("Nested form source", 0),
                       f"/Type /XObject /Subtype /Form /BBox [0 0 612 100] "
                       f"/Resources << /Font << /F1 {font} 0 R >> >>")
    outer = pdf.stream("/Inner Do", f"/Type /XObject /Subtype /Form /BBox [0 0 612 792] "
                       f"/Resources << /XObject << /Inner {inner} 0 R >> >>")
    repeated = pdf.stream(text_at("Repeated form source", 0),
                          f"/Type /XObject /Subtype /Form /BBox [0 0 612 100] "
                          f"/Resources << /Font << /F1 {font} 0 R >> >>")
    resources = f"/XObject << /Outer {outer} 0 R /Repeated {repeated} 0 R >>"
    content = pdf.stream("q 1 0 0 1 0 720 cm /Outer Do Q\n"
                         "q 1 0 0 1 0 680 cm /Repeated Do Q\n"
                         "q 1 0 0 1 0 640 cm /Repeated Do Q")
    if inherited:
        leaf = pdf.add(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents {content} 0 R >>")
        return pdf.finish([leaf], pages_extra=f"/Resources << {resources} >>")
    return pdf.finish([page(pdf, content, resources)])


def acroform() -> bytes:
    pdf = Pdf()
    font = pdf.add(FONT)
    content = pdf.stream(text_at("Application form", 720))
    leaf = page(pdf, content, f"/Font << /F1 {font} 0 R >>")
    fields = [pdf.add(f"<< /Type /Annot /Subtype /Widget /FT /Tx /T ({name}) /V ({value}) "
                      f"/P {leaf} 0 R /Rect [72 {y} 350 {y + 16}] /F 4 >>")
              for name, value, y in [("name", "Mara Example", 680), ("decision", "Approved", 640)]]
    refs = " ".join(f"{field} 0 R" for field in fields)
    pdf.objects[leaf - 1] = pdf.objects[leaf - 1][:-2] + f" /Annots [{refs}] >>".encode()
    return pdf.finish([leaf], catalog_extra=f"/AcroForm << /Fields [{refs}] /NeedAppearances true >>")


def image_page(*, existing_layer: bool) -> bytes:
    pdf = Pdf()
    font = pdf.add(FONT)
    raster = pdf.stream(b"\x80", "/Type /XObject /Subtype /Image /Width 1 /Height 1 "
                        "/ColorSpace /DeviceGray /BitsPerComponent 8")
    content = f"q 612 0 0 792 0 0 cm /Raster Do Q\n"
    if existing_layer:
        content += text_at("Visible invoice header", 720) + "\n"
        content += text_at("OCR invoice 314. Total 27 EUR.", 680, invisible=True)
    info = pdf.add("<< /Title (METADATA IS NOT PAGE TEXT) /Subject (DO NOT INVENT OCR TEXT) >>")
    leaf = page(pdf, pdf.stream(content),
                f"/Font << /F1 {font} 0 R >> /XObject << /Raster {raster} 0 R >>")
    return pdf.finish([leaf], info=info)


def broken_second_page() -> bytes:
    pdf = Pdf()
    font = pdf.add(FONT)
    resources = f"/Font << /F1 {font} 0 R >>"
    first = page(pdf, pdf.stream(text_at("Complete first page is not a complete document", 720)), resources)
    second = page(pdf, pdf.stream(b"ordinary invalid compressed data", "/Filter /FlateDecode"), resources)
    return pdf.finish([first, second])


def fixtures() -> dict[str, bytes]:
    return {
        "winansi": plain_pages([text_at("Café ÅÄÖ åäö", 720)]),
        "multilingual": multilingual(),
        "page_order": plain_pages([
            "\n".join([text_at("1", 30), text_at("First page heading", 720),
                        text_at("<sup>literal</sup> & 5 < 7", 680)]),
            "\n".join([text_at("2", 30), text_at("Second page heading", 720),
                        text_at("Final source sentence.", 680)]),
        ]),
        "nested_forms": forms(),
        "inherited_forms": forms(inherited=True),
        "acroform": acroform(),
        "existing_ocr_layer": image_page(existing_layer=True),
        "image_only": image_page(existing_layer=False),
        "crop_visibility": plain_pages([
            text_at("OUTSIDE CROP MUST BE ABSENT", 100) + "\n" + text_at("Inside crop", 720)
        ], "/CropBox [0 400 612 792]"),
        "blank_page": plain_pages([""]),
        "not_pdf": b"This fixture has no PDF header or document objects.\n",
        "truncated": b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R\n",
        "broken_second_page": broken_second_page(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--check", action="store_true")
    args = parser.parse_args()
    files = fixtures()
    expected_raw = (ROOT / "expected.json").read_bytes()
    expected = json.loads(expected_raw)
    if {case["id"] for case in expected["cases"]} != files.keys():
        raise SystemExit("fixture IDs differ from independently authored expectations")
    manifest = {"format": "recall-pdf-corpus-v1", "license": "MIT OR Apache-2.0",
                "expected_sha256": hashlib.sha256(expected_raw).hexdigest(),
                "files": [{"id": key, "file": f"fixtures/{key}.pdf", "bytes": len(value),
                           "sha256": hashlib.sha256(value).hexdigest()}
                          for key, value in sorted(files.items())]}
    rendered = (json.dumps(manifest, indent=2, ensure_ascii=False) + "\n").encode()
    if args.write:
        (ROOT / "fixtures").mkdir(exist_ok=True)
        for key, value in files.items():
            (ROOT / "fixtures" / f"{key}.pdf").write_bytes(value)
        (ROOT / "manifest.json").write_bytes(rendered)
    else:
        if (ROOT / "manifest.json").read_bytes() != rendered:
            raise SystemExit("manifest differs; do not rewrite frozen expectations to make a parser pass")
        if {p.name for p in (ROOT / "fixtures").iterdir()} != {f"{key}.pdf" for key in files}:
            raise SystemExit("fixture directory contains missing or untracked corpus members")
        for key, value in files.items():
            if (ROOT / "fixtures" / f"{key}.pdf").read_bytes() != value:
                raise SystemExit(f"fixture bytes differ: {key}")
    print(f"{len(files)} deterministic input fixtures; manifest sha256={hashlib.sha256(rendered).hexdigest()}")


if __name__ == "__main__":
    main()
