from google.cloud import storage

client = storage.Client.from_service_account_json("credenciais.json")

buckets = list(client.list_buckets())

print("Conexão OK! Buckets encontrados:")
for bucket in buckets:
    print(bucket.name)