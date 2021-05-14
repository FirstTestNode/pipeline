SELECT *
FROM (
         SELECT T.Source,
                ([Date])                           [Date],
                T.SenderId,
                ISNULL(P.ProviderName, T.SenderId) [Hospital_Clinic],
                C.Name                             [Facility City]
                 ,
                PAR.ProviderAreaName               [Facility Area]
                 ,
                T.ReceiverId,
                CL.ClientName
                 ,
                [CliniName],
                [CliniGender],
                [CliniFacilityName],
                [CliniFacilityID],
                [CliniFacilityLocation],
                [CliniStatus],
                [CliniActiveFrom],
                [CliniActiveTo],
                [CliniMajorDept],
                [CliniMajorCat],
                [CliniMajorSpecial],
                [CliniCountry],
                [CliniLang],
                [CliniLicenseTypeKey],
                [ClinicianID],
                [FacilityName],
                [FacilityLicenseTypeKey],
                [FacilityID],
                [FacilityPhone],
                [FacilityEmail],
                [FacilityCategory],
                [FacilitySpecialization],
                [FacilityRegion],
                FM.[ProviderID]
                 ,
                T.DiagnosisCode
                 ,
                ICD.DiagnosisShortDescription      Diagnosis
                 ,
                T.DrugCode,
                D.TradeName                        BrandName,
                D.[Form]
                 ,
                T.Quantity
                 ,
                MAX(ATC.ATCCode)                   ATCCode,
                MAX(ATC.ATCDescription)            ATC,
                MAX(GenericName)                   GenericName,
                (CASE
                     WHEN S.SourceName = 'DHA' THEN (ISNULL(T.Quantity, 1) / ISNULL(D.GranularUnit, 1))
                     WHEN S.SourceName = 'HAAD' THEN ISNULL(T.Quantity, 1)
                     ELSE T.Quantity
                    END)                           [Unit],
                (CASE
                     WHEN S.SourceName = 'DHA' THEN D.UnitPrice *
                                                    CEILING(ISNULL(T.Quantity, 1) / ISNULL(D.GranularUnit, 1))
                     WHEN S.SourceName = 'HAAD' THEN D.UnitPrice * CEILING(ISNULL(T.Quantity, 1))
                     ELSE D.UnitPrice * (ISNULL(T.Quantity, 1))
                    END)                           [Value]
         FROM (
                  SELECT *
                  FROM vw_report_eRxRequests eRX
                  UNION
                  SELECT *
                  FROM vw_report_PriorRequests PR
                  WHERE NOT EXISTS(SELECT top 1 1
                                   FROM vw_report_eRxRequests eRX
                                   WHERE eRx.AuthorizationID = PR.AuthorizationID)
                  UNION
                  SELECT *
                  FROM vw_report_eClaims CL
                  WHERE NOT EXISTS(SELECT top 1 1
                                   FROM vw_report_eRxRequests eRX
                                   WHERE eRX.AuthorizationID = CL.AuthorizationID)
                    AND NOT EXISTS(SELECT top 1 1
                                   FROM vw_report_PriorRequests PR
                                   WHERE PR.AuthorizationID = CL.AuthorizationID)
--UNION
--SELECT * FROM vw_report_PBM
              ) AS T
                  LEFT JOIN IDDK_Drugs D WITH (NOLOCK) ON T.DrugCode = D.DrugCode
                  LEFT JOIN Diagnosis ICD WITH (NOLOCK) ON T.DiagnosisCode = ICD.DiagnosisCode
                  LEFT JOIN Clients CL WITH (NOLOCK) ON CL.ClientId = T.ClientId
                  LEFT JOIN DrugMapping_Scientifics DMS WITH (NOLOCK) ON DMS.DrugKey = D.DrugKey
                  LEFT JOIN ScientificNameGenerics SG WITH (NOLOCK) ON SG.ScientificNameKey = DMS.ScientificNameKey
                  LEFT JOIN GenericNameATCs GATC WITH (NOLOCK) ON GATC.GenericNameKey = SG.GenericNameKey
                  LEFT JOIN GenericName GEN WITH (NOLOCK) ON GEN.GenericNameKey = SG.GenericNameKey
                  LEFT JOIN ATC ATC WITH (NOLOCK) ON ATC.ATCKey = GATC.ATCKey
                  LEFT JOIN AV_Providers P WITH (NOLOCK) ON P.EclaimLinkID = T.ProviderId --null
                  LEFT JOIN NANO_Brain_RND_Dev..FacilityMaster FM WITH (NOLOCK) ON FM.ProviderID = T.ProviderId --null
                  LEFT JOIN AV_ProviderAreaLink PL WITH (NOLOCK) ON PL.ProviderKey = P.PKey
                  LEFT JOIN AV_ProviderArea PAR WITH (NOLOCK) ON PAR.Pkey = PL.ProviderAreaKey
                  LEFT JOIN AV_City C WITH (NOLOCK) ON C.PKey = P.CityKey--null
                  LEFT JOIN AV_Source S WITH (NOLOCK) ON S.Pkey = P.LicenseSourceKey
                  LEFT JOIN AV_Specialities SP WITH (NOLOCK) ON SP.License = T.Clinician
                  LEFT JOIN NANO_Brain_RND_Dev..ClinicianMaster CLN WITH (NOLOCK) ON CLN.ClinicianID = T.Clinician
--LEFT JOIN AV_Gender G WITH(NOLOCK) ON G.PKey=CLN.GenderKey
         GROUP BY T.SenderId, T.Date, P.ProviderName, T.SenderId, C.Name, PAR.ProviderAreaName,
                  T.ReceiverId, CL.ClientName, [CliniName], [CliniGender], [CliniFacilityName], [CliniFacilityID],
                  [CliniFacilityLocation], [CliniStatus], [CliniActiveFrom], [CliniActiveTo], [CliniMajorDept],
                  [CliniMajorCat], [CliniMajorSpecial], [CliniCountry], [CliniLang], [CliniLicenseTypeKey],
                  [ClinicianID],
                  [FacilityName], [FacilityLicenseTypeKey], [FacilityID], [FacilityPhone],
                  [FacilityEmail], [FacilityCategory], [FacilitySpecialization], [FacilityRegion], FM.[ProviderID],
                  T.DiagnosisCode,
                  T.DrugCode, D.TradeName, D.[Form], T.Quantity, S.SourceName, D.GranularUnit,
                  D.UnitPrice, T.Source, ICD.DiagnosisShortDescription
     ) TBL
WHERE ([Date] BETWEEN '2020-01-01' AND '2021-03-31')
  AND (DrugCode IN
       ('0414-203702-1071',
        '0414-203702-1072',
        '0414-203702-1971',
        '0414-856101-3801',
        '0414-203704-3681',
        '0414-203705-0221',
        'K06-4839-05041-01',
        'K06-4842-05041-01',
        'K06-4839-05041-02',
        'F50-4841-05038-01',
        'F50-4840-05040-01',
        '1571-5485-001',
        'G83-6158-03182-01',
        'G83-6159-03182-01',
        'G83-6159-06124-01',
        'G83-5929-06124-01',
        'G83-6092-06289-01',
        'G83-6158-06124-01',
        'G83-7411-07380-01',
        'G83-7412-07381-01',
        'G83-5224-02889-01',
        'G83-5224-02890-01',
        'K01-3956-05037-01',
        'K01-3956-03175-01',
        'E65-3792-04091-01',
        'M79-7223-07220-01',
        'M79-4983-06573-01',
        'M79-4984-02891-01',
        'M79-4985-06573-01',
        'T95-7621-07513-01',
        'D69-7047-07091-01',
        'D69-2621-05141-01',
        'D69-2619-02887-01',
        'T95-8171-08016-01',
        'D69-2620-02887-01',
        'D69-2619-03183-01',
        'D69-2619-01463-01')
    )