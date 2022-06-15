
ALTER PROCEDURE [dbo].[ETL_ValidateClaims_v2]
(
@ETLEntryID UNIQUEIDENTIFIER NULL = NULL
)
AS
BEGIN
BEGIN TRY
BEGIN TRANSACTION
--error logs clean up
TRUNCATE TABLE [LandingArea].[ETL_LandingErrors];
--GET ETL ENTRY ID
BEGIN
EXEC [dbo].[ETL_CreateETLEntry] @NEWETLEntryID = @ETLEntryID OUTPUT
END;
--CREATE TEMP TABLE
BEGIN
CREATE TABLE #V2_ClaimsValidations_Staging(
[NetworkPartnerCode] [nvarchar](150) NULL,
[ControlNumber] [nvarchar](150) NULL,
[PCCNumber] [nvarchar](150) NULL,
[LocalPolicyNumber] [nvarchar](150) NULL,
[ClaimID] [nvarchar](150) NULL,
[InsuredType] [nvarchar](150) NULL,
[Sex] [nvarchar](150) NULL,
[AgeBand] [nvarchar](150) NULL,
[DateClaimIncurred] [nvarchar](150) NULL,
[DateClaimSubmitted] [nvarchar](150) NULL,
[DateClaimPaid] [nvarchar](150) NULL,
[IncurredClaimAmount] [nvarchar](150) NULL,
[PaidClaimAmount] [nvarchar](150) NULL,
[ICDDiagnosisCode] [nvarchar](150) NULL,
[NameOfProvider] [nvarchar](150) NULL,
[Currency] [nvarchar](150) NULL,
[ClaimantID] [nvarchar](150) NULL,
[ClaimType] [nvarchar](150) NULL,
[ErrorRow] [int],
[RowID] [int],
RowControlID [int]
);
CREATE NONCLUSTERED INDEX [IX_#V2_ClaimsValidations_Staging] ON #V2_ClaimsValidations_Staging
(
[NetworkPartnerCode] ASC,
[ControlNumber] ASC,
[PCCNumber] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY];
CREATE NONCLUSTERED INDEX [IX_#V2_ClaimsValidations_Staging2] ON #V2_ClaimsValidations_Staging
(
[ClaimID] ASC,
[LocalPolicyNumber] ASC,
[ICDDiagnosisCode] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY];UPDATE [LandingArea].[V2_claims_Landing] set RowControlID = a.RowControlID
FROM [LandingArea].[LoadFilesLog] a
WHERE SUBSTRING(A.SourceFileName, 6, 3) = NetworkPartnerCode
AND a.SourceFileName LIKE '%CLAIMS%'
AND a.IsLoaded != 1INSERT INTO #V2_ClaimsValidations_Staging
SELECT [NetworkPartnerCode], [ControlNumber], [PCCNumber], [LocalPolicyNumber], [ClaimID], [InsuredType], [Sex], [AgeBand],
[DateClaimIncurred], [DateClaimSubmitted], [DateClaimPaid], [IncurredClaimAmount], [PaidClaimAmount],
[ICDDiagnosisCode], [NameOfProvider], [CurrencyID], [ClaimantID], [ClaimType], 0, 0, RowControlID
FROM [LandingArea].[V2_Claims_Landing]
ORDER BY [InsertOrder]--ASSIGN ROW NUMBER
UPDATE #V2_ClaimsValidations_Staging
SET #V2_ClaimsValidations_Staging.[RowID] = X.TheId
FROM (SELECT NetworkPartnerCode, ControlNumber, PCCNumber, LocalPolicyNumber, ClaimID, DateClaimIncurred, DateClaimSubmitted, DateClaimPaid,
ROW_NUMBER() OVER (PARTITION BY [NetworkpartnerCode] order by [NetworkpartnerCode]) as TheId
FROM #V2_ClaimsValidations_Staging) X
WHERE #V2_ClaimsValidations_Staging.NetworkPartnerCode = X.NetworkPartnerCode
AND #V2_ClaimsValidations_Staging.ControlNumber = X.ControlNumber
AND #V2_ClaimsValidations_Staging.PCCNumber = X.PCCNumber
AND isnull(#V2_ClaimsValidations_Staging.LocalPolicyNumber,'null') = isnull(X.LocalPolicyNumber,'null')
AND isnull(#V2_ClaimsValidations_Staging.ClaimID,'null') = isnull(X.ClaimID,'null')
AND isnull(#V2_ClaimsValidations_Staging.DateClaimIncurred,'null') = isnull(X.DateClaimIncurred,'null')
AND isnull(#V2_ClaimsValidations_Staging.DateClaimSubmitted,'null') = isnull(X.DateClaimSubmitted,'null')
AND isnull(#V2_ClaimsValidations_Staging.DateClaimPaid,'null') = isnull(X.DateClaimPaid,'null')END;--START VALIDATION
--INSERT INTO THE ERROR TABLE;THEN SET FLAG ON THE TEMP TABLE
BEGIN--DELETE LATER-sample error;
select 1;
throw 6000, 'CUSTOM ERROR', 1
--INSERT INTO [LandingArea].[ETL_LandingErrors]
--([Process], [RowID], [NetworkPartnerCode], [ControlNumber], [PCCNumber], [LocalPolicyNumber], [ErrorMessage], [RowControlID], [Created_ETLEntryID])
--SELECT TOP 1 'LOAD CLAIMS', RowID, NetworkPartnerCode, ControlNumber, PCCNumber, LocalPolicyNumber,
--concat('SAMPLE ERROR CODE MESSAGE ', [NetworkPartnerCode] ,'", Row "', RowID+1, '"')
--AS [ErrorMessage], RowControlID, @ETLEntryID
--FROM #V2_ClaimsValidations_StagingEND;
--THIS PART TOCOMMIT
END TRY
BEGIN CATCH
ROLLBACK;
DECLARE @ErrLog [varchar](250);
select @ErrLog = CONCAT('TRANSACTION ERROR: ',
ERROR_MESSAGE(),';',
ERROR_PROCEDURE(), ';',
ERROR_LINE()
);
INSERT INTO [LandingArea].[ETL_LandingErrors] ([Process], [RowID], [NetworkPartnerCode], [ControlNumber], [PCCNumber], [LocalPolicyNumber], [ErrorMessage], [RowControlID], [Created_ETLEntryID]) values ('Validate-Catch', -1, -1, -1, -1, -1, @ErrLog, -1, @ETLEntryID)
--do i need to throw?
END CATCH
END

