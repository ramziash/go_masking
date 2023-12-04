package main

import (
	"fmt"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type ParquetRec struct {
	AgreementId             string `parquet:"name=agreementId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AgreementSourceSystemId string `parquet:"name=agreementSourceSystemId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PartyIdScheme           string `parquet:"name=partyIdScheme, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PartyIdValue            string `parquet:"name=partyIdValue, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SourceSystemId          string `parquet:"name=sourceSystemId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Snapshot_date           string `parquet:"name=snapshot_date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

// type ParquetRec struct {
// 	AccountingCategoryId                               string `parquet:"name=accountingCategoryId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	AccountingUnitId                                   string `parquet:"name=accountingUnitId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	AccountingUnitSourceSystemId                       string `parquet:"name=accountingUnitSourceSystemId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	CurrencyId                                         string `parquet:"name=currencyId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	ManagedByOrganisationUnitId                        string `parquet:"name=managedByOrganisationUnitId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	ManagedByOrganisationUnitSourceSystemId            string `parquet:"name=managedByOrganisationUnitSourceSystemId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	ProductAgreementAgreementId                        string `parquet:"name=productAgreementAgreementId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	ProductAgreementAgreemenSourceSystemtId            string `parquet:"name=productAgreementAgreemenSourceSystemtId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	ProductAgreementAgreementTypeId                    string `parquet:"name=productAgreementAgreementTypeId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	AccountingUnitLifeCycleStatusLifeCycleStatusTypeId string `parquet:"name=accountingUnitLifeCycleStatusLifeCycleStatusTypeId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	AccountingUnitLifeCycleStatusLifeCycleStatusDate   string `parquet:"name=accountingUnitLifeCycleStatusLifeCycleStatusDate, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	OriginationProcessId                               string `parquet:"name=originationProcessId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	SourceSystemId                                     string `parquet:"name=sourceSystemId, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// 	Snapshot_date                                      string `parquet:"name=snapshot_date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
// }

func ReadParquetInChunks(filePath string, chunkChan chan<- [][]string, chunkSize int) error {
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return err
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, int64(chunkSize))
	if err != nil {
		return err
	}
	defer pr.ReadStop()

	numRows := int(pr.GetNumRows())
	// numCols := int(pr.SchemaHandler.GetColumnNum())

	for start := 0; start < numRows; start += chunkSize {
		end := start + chunkSize
		if end > numRows {
			end = numRows
		}

		rows := make([]ParquetRec, end-start)
		if err := pr.Read(&rows); err != nil {
			return fmt.Errorf("error at chunk %d: %v", start, err)
		}

		chunk := make([][]string, end-start)

		for i, row := range rows {

			chunk[i] = []string{
				row.AgreementId,
				row.AgreementSourceSystemId,
				row.PartyIdScheme,
				row.PartyIdValue,
				row.SourceSystemId,
				row.Snapshot_date,
			}

		}

		chunkChan <- chunk
	}

	close(chunkChan)
	return nil
}

// chunk[i] = []string{
// 	row.AgreementId,
// 	row.AgreementSourceSystemId,
// 	row.PartyIdScheme,
// 	row.PartyIdValue,
// 	row.SourceSystemId,
// 	row.Snapshot_date,
// }

// chunk[i] = []string{
// 	row.AccountingCategoryId,
// 	row.AccountingUnitId,
// 	row.AccountingUnitSourceSystemId,
// 	row.CurrencyId,
// 	row.ManagedByOrganisationUnitId,
// 	row.ManagedByOrganisationUnitSourceSystemId,
// 	row.ProductAgreementAgreementId,
// 	row.ProductAgreementAgreemenSourceSystemtId,
// 	row.ProductAgreementAgreementTypeId,
// 	row.AccountingUnitLifeCycleStatusLifeCycleStatusTypeId,
// 	row.AccountingUnitLifeCycleStatusLifeCycleStatusDate,
// 	row.OriginationProcessId,
// 	row.SourceSystemId,
// 	row.Snapshot_date,
// }
