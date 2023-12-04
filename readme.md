The go app requires an imporovement & use GORM to adentify the schemas

The package will need a .parquet file to be available in the same directory.

Make sure that the .parquet file schema is properly setup in the read_parquet.go

Run the below to create an image.

```
docker build --tag masking_golang

docker build --platform amd64 -t masking_golang:v1.0.0 --no-cache .

```

```
here to tag it. the azurerm info will run from terraform. its here just for reference.

docker tag masking_golang:v1.0.0 ${azurerm_container_registry.acr.name}.azurecr.io/masking_golang:v1.0.0
```
