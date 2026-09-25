Bu klasör botun kendi pozisyon defterini (ledger) tutar.

positions.json burada botun ilk gerçek satış/alış turunda otomatik oluşturulur; elle oluşturmanıza gerek yoktur.

Gerçek ortalama maliyetinizi biliyorsanız, ilk çalıştırmadan ÖNCE positions.json dosyasını şu formatta oluşturup commitleyebilirsiniz:

{
  "lots": [
    {"qty": 0.00098296, "price": 1180.0, "ts": 0},
    {"qty": 0.00099299, "price": 1195.0, "ts": 0}
  ],
  "total_owned": null,
  "pending_buy_price": null,
  "pending_sell_price": null
}

total_owned alanını null bırakın; bot ilk çalıştırmada bunu gerçek zincir durumuna göre kendisi dolduracaktır.
