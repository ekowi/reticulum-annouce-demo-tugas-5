"""Demo pesan satu paket Reticulum 1.5.5: node B menerima, node A mengirim."""
import argparse
import os
from pathlib import Path
import sys
import threading
import time
from datetime import datetime, timezone, timedelta

import RNS

APP_NAME = "tugas_reticulum"
ASPECT = "pesan"
WIB = timezone(timedelta(hours=7))


def log(role, text):
    stamp = datetime.now(WIB).strftime("%H:%M:%S")
    print(f"[{stamp} WIB] [{role}] {text}", flush=True)


class AnnounceHandler:
    aspect_filter = APP_NAME + "." + ASPECT
    receive_path_responses = True

    def received_announce(self, destination_hash, announced_identity, app_data,
                         announce_packet_hash, is_path_response):
        kind = "PATH_RESPONSE" if is_path_response else "ANNOUNCE"
        log("A", f"{kind} valid diterima: {destination_hash.hex()}")


def receive(args):
    config = Path(args.config)
    config.mkdir(parents=True, exist_ok=True)
    identity_file = config / "identitas_B"
    if identity_file.exists():
        identity = RNS.Identity.from_file(str(identity_file))
        if identity is None:
            raise RuntimeError("Identitas B tidak dapat dibaca")
    else:
        identity = RNS.Identity()
        identity.to_file(str(identity_file))

    destination = RNS.Destination(identity, RNS.Destination.IN,
                                  RNS.Destination.SINGLE, APP_NAME, ASPECT)
    received = threading.Event()

    def on_packet(data, packet):
        log("B", "PESAN DITERIMA: " + data.decode("utf-8", errors="replace"))
        log("B", "Hash paket: " + packet.get_hash().hex())
        received.set()

    destination.set_packet_callback(on_packet)
    destination.set_proof_strategy(RNS.Destination.PROVE_ALL)
    destination.set_default_app_data(b"Node B - penerima tugas")
    (config / "destination_B.txt").write_text(destination.hash.hex(), encoding="utf-8")
    log("B", "Destination: " + destination.hash.hex())
    log("B", "Siap menerima; proof penerimaan otomatis diaktifkan")
    # Jeda singkat memberi kesempatan interface TCP tersambung dalam demo lokal.
    time.sleep(1)
    destination.announce()
    log("B", "ANNOUNCE dikirim")
    if not received.wait(args.timeout):
        raise TimeoutError("Tidak ada pesan sampai batas waktu")
    # Callback menandai penerimaan; Transport mengirim proof sesudah callback.
    time.sleep(2)
    log("B", "Selesai menerima satu pesan")


def send(args):
    if not args.destination:
        raise ValueError("Mode send memerlukan --destination HASH_B")
    target = bytes.fromhex(args.destination)
    if len(target) != RNS.Reticulum.TRUNCATED_HASHLENGTH // 8:
        raise ValueError("Destination harus berupa 32 karakter heksadesimal")
    RNS.Transport.register_announce_handler(AnnounceHandler())
    log("A", "Tujuan: " + target.hex())
    if not RNS.Transport.has_path(target):
        log("A", "Jalur belum ada; mengirim PATH REQUEST")
        RNS.Transport.request_path(target)
    deadline = time.monotonic() + args.timeout
    while not RNS.Transport.has_path(target):
        if time.monotonic() >= deadline:
            raise TimeoutError("Jalur ke B tidak ditemukan")
        time.sleep(0.05)

    identity = RNS.Identity.recall(target)
    if identity is None:
        raise RuntimeError("Kunci publik B belum diketahui")
    destination = RNS.Destination(identity, RNS.Destination.OUT,
                                  RNS.Destination.SINGLE, APP_NAME, ASPECT)
    if destination.hash != target:
        raise ValueError("Destination tidak cocok dengan nama aplikasi/aspect")
    log("A", f"Jalur ditemukan: {RNS.Transport.hops_to(target)} hop")
    log("A", "Interface: " + str(RNS.Transport.next_hop_interface(target)))
    payload = args.message.encode("utf-8")
    # Demo ini mengirim satu paket. Gunakan Link/Resource untuk data besar.
    if len(payload) > RNS.Packet.ENCRYPTED_MDU:
        raise ValueError(f"Pesan melebihi {RNS.Packet.ENCRYPTED_MDU} byte")
    packet = RNS.Packet(destination, payload)
    receipt = packet.send()
    if not receipt:
        raise RuntimeError("Paket gagal dikirim")
    receipt.set_timeout(args.timeout)
    log("A", "PESAN DIKIRIM: " + args.message)
    log("A", "Hash paket: " + packet.get_hash().hex())
    deadline = time.monotonic() + args.timeout
    while receipt.get_status() == RNS.PacketReceipt.SENT:
        if time.monotonic() >= deadline:
            raise TimeoutError("Bukti penerimaan belum diterima")
        time.sleep(0.05)
    if receipt.get_status() != RNS.PacketReceipt.DELIVERED:
        raise RuntimeError("Penerimaan paket tidak terkonfirmasi")
    log("A", "DELIVERED: proof dari B berhasil diverifikasi")
    log("A", f"Waktu pulang-pergi proof: {receipt.get_rtt() * 1000:.2f} ms")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["receive", "send"])
    parser.add_argument("--config", required=True, help="Folder konfigurasi Reticulum")
    parser.add_argument("--destination", help="Hash destination B")
    parser.add_argument("--message", default="Halo Node B, ini pesan dari Node A!")
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout harus positif")
    role = "B" if args.mode == "receive" else "A"
    try:
        stack = RNS.Reticulum(configdir=args.config, loglevel=RNS.LOG_ERROR)
        log(role, f"Reticulum {RNS.__version__}; proses PID={os.getpid()}")
        receive(args) if args.mode == "receive" else send(args)
        return 0
    except (ValueError, RuntimeError, TimeoutError, OSError) as error:
        log(role, "GAGAL: " + str(error))
        return 1
    except KeyboardInterrupt:
        log(role, "Dihentikan pengguna")
        return 130


if __name__ == "__main__":
    sys.exit(main())
