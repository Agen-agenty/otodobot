---
description: cek dan kelola status server lokal (n8n, embedding, Infinity DB)
---

Gunakan agen `operator` untuk memeriksa kondisi server, lalu laporkan hasilnya dalam bahasa Indonesia.

Jalankan `/root/infinity-n8n/status.sh` terlebih dahulu. Jika semua layanan OK, tampilkan ringkasan alamat localhost dan WiFi. Jika ada yang DOWN, baca log terkait di `/root/infinity-n8n/logs/`, jelaskan penyebab yang paling mungkin, dan tawarkan `start-all.sh` sebagai perbaikan — jangan menjalankan perintah yang mengubah keadaan tanpa konfirmasi.

<!-- Copyright (c) 2026 Tom05ivier-commits -->
