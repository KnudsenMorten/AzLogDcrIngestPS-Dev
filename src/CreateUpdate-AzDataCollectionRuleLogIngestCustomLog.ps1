Function CreateUpdate-AzDataCollectionRuleLogIngestCustomLog
{
 <#
    .SYNOPSIS
    Create or Update Azure Data Collection Rule (DCR) used for log ingestion to Azure LogAnalytics using Log Ingestion API

    .DESCRIPTION
    Uses schema based on source object

    .AUTHOR
    Morten Knudsen, Microsoft MVP - https://mortenknudsen.net

    .LINK
    https://github.com/KnudsenMorten/AzLogDcrIngestPS

    .PARAMETER Tablename
    Specifies the table name in LogAnalytics

    .PARAMETER SchemaSourceObject
    This is the schema in hash table format coming from the source object

    .PARAMETER SchemaMode
    SchemaMode = Merge (default)
    It will do a merge/union of new properties and existing schema properties. DCR will import schema from table

    SchemaMode = Overwrite
    It will overwrite existing schema in DCR/table � based on source object schema
    This parameter can be useful for separate overflow work

    SchemaMode = Migrate
    It will create the DCR, based on the schema from the LogAnalytics v1 table schema
    This parameter is used only as part of migration away from HTTP Data Collector API to Log Ingestion API

    .PARAMETER AzLogWorkspaceResourceId
    This is the Loganaytics Resource Id

    .PARAMETER DceName
    This is name of the Data Collection Endpoint to use for the upload
    Function will automatically look check in a global variable ($global:AzDceDetails) - or do a query using Azure Resource Graph to find DCE with name
    Goal is to find the log ingestion Uri on the DCE

    Variable $global:AzDceDetails can be build before calling this cmdlet using this syntax
    $global:AzDceDetails = Get-AzDceListAll -AzAppId $LogIngestAppId -AzAppSecret $LogIngestAppSecret -TenantId $TenantId -Verbose:$Verbose -Verbose:$Verbose
 
    .PARAMETER DcrResourceGroup
    This is name of the resource group, where Data Collection Rules will be stored

    .PARAMETER DcrName
    This is name of the Data Collection Rule to use for the upload
    Function will automatically look check in a global variable ($global:AzDcrDetails) - or do a query using Azure Resource Graph to find DCR with name
    Goal is to find the DCR immunetable id on the DCR

    Variable $global:AzDcrDetails can be build before calling this cmdlet using this syntax
    $global:AzDcrDetails = Get-AzDcrListAll -AzAppId $LogIngestAppId -AzAppSecret $LogIngestAppSecret -TenantId $TenantId -Verbose:$Verbose -Verbose:$Verbose

    .PARAMETER TableName
    This is tablename of the LogAnalytics table (and is also used in the DCR naming)

    .PARAMETER AzDcrSetLogIngestApiAppPermissionsDcrLevel
    Choose TRUE if you want to set Monitoring Publishing Contributor permissions on DCR level
    Choose FALSE if you would like to use inherited permissions from the resource group level (recommended)

    .PARAMETER LogIngestServicePricipleObjectId
    This is the object id of the Azure App service-principal
    NOTE: Not the object id of the Azure app, but Object Id of the service principal (!)

    .PARAMETER AzAppId
    This is the Azure app id
        
    .PARAMETER AzAppSecret
    This is the secret of the Azure app

    .PARAMETER TenantId
    This is the Azure AD tenant id

    .INPUTS
    None. You cannot pipe objects

    .OUTPUTS
    Output of REST PUT command. Should be 200 for success

    .EXAMPLE
    #-------------------------------------------------------------------------------------------
    # Variables
    #-------------------------------------------------------------------------------------------
    $verbose                                         = $true

    $TenantId                                        = "xxxxx" 
    $LogIngestAppId                                  = "xxxxx" 
    $LogIngestAppSecret                              = "xxxxx" 

    $DceName                                         = "dce-log-platform-management-client-demo1-p" 
    $LogAnalyticsWorkspaceResourceId                 = "/subscriptions/xxxxxx/resourceGroups/rg-logworkspaces/providers/Microsoft.OperationalInsights/workspaces/log-platform-management-client-demo1-p" 
    $AzDcrPrefixClient                               = "clt1" 

    $AzDcrSetLogIngestApiAppPermissionsDcrLevel      = $false
    $AzDcrLogIngestServicePrincipalObjectId          = "xxxxxx" 

    #-------------------------------------------------------------------------------------------
    # Collecting data (in)
    #-------------------------------------------------------------------------------------------
            
    Write-Output ""
    Write-Output "Collecting OS information ... Please Wait !"

    $DataVariable = Get-CimInstance -ClassName Win32_OperatingSystem

    #-------------------------------------------------------------------------------------------
    # Preparing data structure
    #-------------------------------------------------------------------------------------------

    # convert CIM array to PSCustomObject and remove CIM class information
    $DataVariable = Convert-CimArrayToObjectFixStructure -data $DataVariable -Verbose:$Verbose
    
    # add CollectionTime to existing array
    $DataVariable = Add-CollectionTimeToAllEntriesInArray -Data $DataVariable -Verbose:$Verbose

    # add Computer & UserLoggedOn info to existing array
    $DataVariable = Add-ColumnDataToAllEntriesInArray -Data $DataVariable -Column1Name Computer -Column1Data $Env:ComputerName  -Column2Name UserLoggedOn -Column2Data $UserLoggedOn

    # Validating/fixing schema data structure of source data
    $DataVariable = ValidateFix-AzLogAnalyticsTableSchemaColumnNames -Data $DataVariable -Verbose:$Verbose

    # Aligning data structure with schema (requirement for DCR)
    $DataVariable = Build-DataArrayToAlignWithSchema -Data $DataVariable -Verbose:$Verbose

    # We change the tablename to something - for example add TEST (InvClientComputerOSInfoTESTV2) - table doesn't exist
    $TableName = 'InvClientComputerOSInfoTESTV2'   # must not contain _CL
    $DcrName   = "dcr-" + $AzDcrPrefixClient + "-" + $TableName + "_CL"

    $Schema = Get-ObjectSchemaAsArray -Data $DataVariable
    $StructureCheck = Get-AzLogAnalyticsTableAzDataCollectionRuleStatus -AzLogWorkspaceResourceId $LogAnalyticsWorkspaceResourceId -TableName $TableName -DcrName $DcrName -SchemaSourceObject $Schema `
                                                                        -AzAppId $LogIngestAppId -AzAppSecret $LogIngestAppSecret -TenantId $TenantId -Verbose:$Verbose


    # we see that structure is missing, so we set the flag to enforce creating both DCR and table
    $StructureCheck

    #-------------------------------------------------------------------------------------------
    # Output
    #-------------------------------------------------------------------------------------------
    VERBOSE:   Checking LogAnalytics table and Data Collection Rule configuration .... Please Wait !
    VERBOSE: GET with 0-byte payload
    VERBOSE:   LogAnalytics table wasn't found !
    VERBOSE:   DCR was not found [ dcr-clt1-InvClientComputerOSInfoTESTV2_CL ]
    $True

    # build schema to be used for LogAnalytics Table
    $Schema = Get-ObjectSchemaAsHash -Data $DataVariable -ReturnType Table -Verbose:$Verbose

    CreateUpdate-AzLogAnalyticsCustomLogTableDcr -AzLogWorkspaceResourceId $LogAnalyticsWorkspaceResourceId -SchemaSourceObject $Schema -TableName $TableName `
                                                    -AzAppId $LogIngestAppId -AzAppSecret $LogIngestAppSecret -TenantId $TenantId -Verbose:$Verbose 

    # build schema to be used for DCR
    $Schema = Get-ObjectSchemaAsHash -Data $DataVariable -ReturnType DCR

    CreateUpdate-AzDataCollectionRuleLogIngestCustomLog -AzLogWorkspaceResourceId $LogAnalyticsWorkspaceResourceId -SchemaSourceObject $Schema `
                                                        -DceName $DceName -DcrName $DcrName -TableName $TableName `
                                                        -LogIngestServicePricipleObjectId  $AzDcrLogIngestServicePrincipalObjectId `
                                                        -AzDcrSetLogIngestApiAppPermissionsDcrLevel $AzDcrSetLogIngestApiAppPermissionsDcrLevel `
                                                        -AzAppId $LogIngestAppId -AzAppSecret $LogIngestAppSecret -TenantId $TenantId -Verbose:$Verbose


    #-------------------------------------------------------------------------------------------
    # Output
    #-------------------------------------------------------------------------------------------
    VERBOSE: Found required DCE info using Azure Resource Graph
    VERBOSE: 
    VERBOSE: GET with 0-byte payload
    VERBOSE: received 898-byte response of content type application/json; charset=utf-8
    VERBOSE: Found required LogAnalytics info
    VERBOSE: 
    VERBOSE: GET with 0-byte payload
    VERBOSE: received 291-byte response of content type application/json; charset=utf-8
    VERBOSE: 
    VERBOSE: Creating/updating DCR [ dcr-clt1-InvClientComputerOSInfoTESTV2_CL ] with limited payload
    VERBOSE: /subscriptions/fce4f282-fcc6-43fb-94d8-bf1701b862c3/resourceGroups/rg-dcr-log-platform-management-client-demo1-p/providers/micros
    oft.insights/dataCollectionRules/dcr-clt1-InvClientComputerOSInfoTESTV2_CL
    VERBOSE: PUT with -1-byte payload
    VERBOSE: received 2033-byte response of content type application/json; charset=utf-8


    StatusCode        : 200
    StatusDescription : OK
    Content           : {"properties":{"immutableId":"dcr-0189d991f81f43efbcfb6fc520541452","dataCollectionEndpointId":"/subscriptions/fce4f2
                        82-fcc6-43fb-94d8-bf1701b862c3/resourceGroups/rg-dce-log-platform-management-client...
    RawContent        : HTTP/1.1 200 OK
                        Pragma: no-cache
                        Vary: Accept-Encoding
                        x-ms-ratelimit-remaining-subscription-resource-requests: 149
                        Request-Context: appId=cid-v1:2bbfbac8-e1b0-44af-b9c6-3a40669d37e3
                        x-ms-correla...
    Forms             : {}
    Headers           : {[Pragma, no-cache], [Vary, Accept-Encoding], [x-ms-ratelimit-remaining-subscription-resource-requests, 149], [Reques
                        t-Context, appId=cid-v1:2bbfbac8-e1b0-44af-b9c6-3a40669d37e3]...}
    Images            : {}
    InputFields       : {}
    Links             : {}
    ParsedHtml        : mshtml.HTMLDocumentClass
    RawContentLength  : 2033

    VERBOSE: 
    VERBOSE: Updating DCR [ dcr-clt1-InvClientComputerOSInfoTESTV2_CL ] with full schema
    VERBOSE: /subscriptions/fce4f282-fcc6-43fb-94d8-bf1701b862c3/resourceGroups/rg-dcr-log-platform-management-client-demo1-p/providers/micros
    oft.insights/dataCollectionRules/dcr-clt1-InvClientComputerOSInfoTESTV2_CL
    VERBOSE: PUT with -1-byte payload
    VERBOSE: received 4485-byte response of content type application/json; charset=utf-8
    StatusCode        : 200
    StatusDescription : OK
    Content           : {"properties":{"immutableId":"dcr-0189d991f81f43efbcfb6fc520541452","dataCollectionEndpointId":"/subscriptions/fce4f2
                        82-fcc6-43fb-94d8-bf1701b862c3/resourceGroups/rg-dce-log-platform-management-client...
    RawContent        : HTTP/1.1 200 OK
                        Pragma: no-cache
                        Vary: Accept-Encoding
                        x-ms-ratelimit-remaining-subscription-resource-requests: 148
                        Request-Context: appId=cid-v1:2bbfbac8-e1b0-44af-b9c6-3a40669d37e3
                        x-ms-correla...
    Forms             : {}
    Headers           : {[Pragma, no-cache], [Vary, Accept-Encoding], [x-ms-ratelimit-remaining-subscription-resource-requests, 148], [Reques
                        t-Context, appId=cid-v1:2bbfbac8-e1b0-44af-b9c6-3a40669d37e3]...}
    Images            : {}
    InputFields       : {}
    Links             : {}
    ParsedHtml        : mshtml.HTMLDocumentClass
    RawContentLength  : 4485

    VERBOSE: 
    VERBOSE: Waiting 10 sec to let Azure sync up so DCR rule can be retrieved from Azure Resource Graph
    VERBOSE: 
    VERBOSE: Getting Data Collection Rules from Azure Resource Graph .... Please Wait !
    VERBOSE: POST with -1-byte payload
    VERBOSE: received 203914-byte response of content type application/json; charset=utf-8
 #>

    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [array]$SchemaSourceObject,
            [Parameter(mandatory)]
                [string]$AzLogWorkspaceResourceId,
            [Parameter(mandatory)]
                [string]$DceName,
            [Parameter(mandatory)]
                [string]$DcrResourceGroup,
            [Parameter(mandatory)]
                [string]$DcrName,
            [Parameter(mandatory)]
                [string]$TableName,
            [Parameter(mandatory)]
                [boolean]$AzDcrSetLogIngestApiAppPermissionsDcrLevel = $false,
            [Parameter()]
                [AllowEmptyCollection()]
                [string]$LogIngestServicePricipleObjectId,
            [Parameter()]
                [string]$SchemaMode = "Merge",  # Merge/Migrate = Merge new properties into existing schema, Overwrite = use source object schema, Migrate = It will create the DCR, based on the schema from the LogAnalytics v1 table schema
            [Parameter()]
                [string]$AzAppId,
            [Parameter()]
                [string]$AzAppSecret,
            [Parameter()]
                [string]$TenantId
         )

    #--------------------------------------------------------------------------
    # Connection
    #--------------------------------------------------------------------------
        $Headers = Get-AzAccessTokenManagement -AzAppId $AzAppId `
                                               -AzAppSecret $AzAppSecret `
                                               -TenantId $TenantId -Verbose:$Verbose

    #--------------------------------------------------------------------------
    # Get DCEs from Azure Resource Graph
    #--------------------------------------------------------------------------
        
        If ($DceName)
            {
                If ($global:AzDceDetails)   # global variables was defined. Used to mitigate throttling in Azure Resource Graph (free service)
                    {
                        # Retrieve DCE in scope
                        $DceInfo = $global:AzDceDetails | Where-Object { $_.name -eq $DceName }
                            If (!($DceInfo))
                                {
                                    Write-Output "Could not find DCE with name [ $($DceName) ]"
                                }
                    }
                Else
                    {
                        $AzGraphQuery = @{
                                            'query' = 'Resources | where type =~ "microsoft.insights/datacollectionendpoints" '
                                         } | ConvertTo-Json -Depth 20

                        $ResponseData = @()

                        $AzGraphUri          = "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2021-03-01"
                        $ResponseRaw         = invoke-webrequest -UseBasicParsing -Method POST -Uri $AzGraphUri -Headers $Headers -Body $AzGraphQuery
                        $ResponseData       += $ResponseRaw.content
                        $ResponseNextLink    = $ResponseRaw."@odata.nextLink"

                        While ($ResponseNextLink -ne $null)
                            {
                                $ResponseRaw         = invoke-webrequest -UseBasicParsing -Method POST -Uri $AzGraphUri -Headers $Headers -Body $AzGraphQuery
                                $ResponseData       += $ResponseRaw.content
                                $ResponseNextLink    = $ResponseRaw."@odata.nextLink"
                            }
                        $DataJson = $ResponseData | ConvertFrom-Json
                        $Data     = $DataJson.data

                        # Retrieve DCE in scope
                        $DceInfo = $Data | Where-Object { $_.name -eq $DceName }
                            If (!($DceInfo))
                                {
                                    Write-Output "Could not find DCE with name [ $($DceName) ]"
                                }
                    }
            }

        # DCE ResourceId (target for DCR ingestion)
        $DceResourceId  = $DceInfo.id
        If ($DceInfo)
            {
                Write-Verbose "Found required DCE info using Azure Resource Graph"
                Write-Verbose ""
            }

    #------------------------------------------------------------------------------------------------
    # Getting LogAnalytics Info
    #------------------------------------------------------------------------------------------------
                
        $LogWorkspaceUrl = "https://management.azure.com" + $AzLogWorkspaceResourceId + "?api-version=2021-12-01-preview"
        $LogWorkspaceId = (invoke-restmethod -UseBasicParsing -Uri $LogWorkspaceUrl -Method GET -Headers $Headers).properties.customerId
        If ($LogWorkspaceId)
            {
                Write-Verbose "Found required LogAnalytics info"
                Write-Verbose ""
            }
                
    #------------------------------------------------------------------------------------------------
    # Build variables
    #------------------------------------------------------------------------------------------------

        # build variables
        $KustoDefault                               = "source | extend TimeGenerated = now()"
        $StreamNameFull                             = "Custom-" + $TableName + "_CL"

        # streamname must be 52 characters or less
        If ($StreamNameFull.length -gt 52)
            {
                $StreamName                         = $StreamNameFull.Substring(0,52)
            }
        Else
            {
                $StreamName                         = $StreamNameFull
            }

        $DceLocation                                = $DceInfo.location

        $DcrSubscription                            = ($AzLogWorkspaceResourceId -split "/")[2]
        $DcrLogWorkspaceName                        = ($AzLogWorkspaceResourceId -split "/")[-1]
        $DcrResourceId                              = "/subscriptions/$($DcrSubscription)/resourceGroups/$($DcrResourceGroup)/providers/microsoft.insights/dataCollectionRules/$($DcrName)"


    #--------------------------------------------------------------------------
    # Get existing DCR, if found
    #--------------------------------------------------------------------------

        $Uri = "https://management.azure.com" + "$DcrResourceId" + "?api-version=2022-06-01"
        $Dcr = $null
        Try
            {
                $Dcr = invoke-webrequest -UseBasicParsing -Uri $Uri -Method GET -Headers $Headers
            }
        Catch
            {
            }


    
    #--------------------------------------------------------------------------
    # DCR was NOT found (create) - or we do an Overwrite
    #--------------------------------------------------------------------------
        If ( (!($Dcr) -and ( ($SchemaMode -eq "Overwrite") -or ($SchemaMode -eq "Merge") ) ) -or ($SchemaMode -eq "Overwrite") )
            {
                #--------------------------------------------------------------------------
                # build initial payload to create DCR for log ingest (api) to custom logs
                #--------------------------------------------------------------------------

                    If ($SchemaSourceObject.count -gt 10)
                        {
                            $SchemaSourceObjectLimited = $SchemaSourceObject[0..10]
                        }
                    Else
                        {
                            $SchemaSourceObjectLimited = $SchemaSourceObject
                        }


                    $DcrObject = [pscustomobject][ordered]@{
                                    properties = @{
                                                    dataCollectionEndpointId = $DceResourceId
                                                    streamDeclarations = @{
                                                                            $StreamName = @{
	  				                                                                            columns = @(
                                                                                                            $SchemaSourceObjectLimited
                                                                                                           )
                                                                                           }
                                                                          }
                                                    destinations = @{
                                                                        logAnalytics = @(
                                                                                            @{ 
                                                                                                workspaceResourceId = $AzLogWorkspaceResourceId
                                                                                                workspaceId = $LogWorkspaceId
                                                                                                name = $DcrLogWorkspaceName
                                                                                             }
                                                                                        ) 

                                                                    }
                                                    dataFlows = @(
                                                                    @{
                                                                        streams = @(
                                                                                        $StreamName
                                                                                   )
                                                                        destinations = @(
                                                                                            $DcrLogWorkspaceName
                                                                                        )
                                                                        transformKql = $KustoDefault
                                                                        outputStream = $StreamName
                                                                     }
                                                                 )
                                                    }
                                    location = $DceLocation
                                    name = $DcrName
                                    type = "Microsoft.Insights/dataCollectionRules"
                                }

                #--------------------------------------------------------------------------
                # create initial DCR using payload
                #--------------------------------------------------------------------------

                    Write-Verbose ""
                    Write-Verbose "Creating/updating DCR [ $($DcrName) ] with limited payload"
                    Write-Verbose $DcrResourceId

                    $DcrPayload = $DcrObject | ConvertTo-Json -Depth 20

                    $Uri = "https://management.azure.com" + "$DcrResourceId" + "?api-version=2022-06-01"
                    invoke-webrequest -UseBasicParsing -Uri $Uri -Method PUT -Body $DcrPayload -Headers $Headers
        
                    # sleeping to let API sync up before modifying
                    Start-Sleep -s 5

                #--------------------------------------------------------------------------
                # build full payload to create DCR for log ingest (api) to custom logs
                #--------------------------------------------------------------------------
                
                    $DcrObject = [pscustomobject][ordered]@{
                                    properties = @{
                                                    dataCollectionEndpointId = $DceResourceId
                                                    streamDeclarations = @{
                                                                            $StreamName = @{
	  				                                                                            columns = @(
                                                                                                            $SchemaSourceObject
                                                                                                           )
                                                                                           }
                                                                          }
                                                    destinations = @{
                                                                        logAnalytics = @(
                                                                                            @{ 
                                                                                                workspaceResourceId = $AzLogWorkspaceResourceId
                                                                                                workspaceId = $LogWorkspaceId
                                                                                                name = $DcrLogWorkspaceName
                                                                                             }
                                                                                        ) 

                                                                    }
                                                    dataFlows = @(
                                                                    @{
                                                                        streams = @(
                                                                                        $StreamName
                                                                                   )
                                                                        destinations = @(
                                                                                            $DcrLogWorkspaceName
                                                                                        )
                                                                        transformKql = $KustoDefault
                                                                        outputStream = $StreamName
                                                                     }
                                                                 )
                                                    }
                                    location = $DceLocation
                                    name = $DcrName
                                    type = "Microsoft.Insights/dataCollectionRules"
                                }

                #--------------------------------------------------------------------------
                # create DCR using payload
                #--------------------------------------------------------------------------

                    Write-Verbose ""
                    Write-Verbose "Updating DCR [ $($DcrName) ] with full payload"
                    Write-Verbose $DcrResourceId

                    $DcrPayload = $DcrObject | ConvertTo-Json -Depth 20

                    $Uri = "https://management.azure.com" + "$DcrResourceId" + "?api-version=2022-06-01"
                    invoke-webrequest -UseBasicParsing -Uri $Uri -Method PUT -Body $DcrPayload -Headers $Headers


                #--------------------------------------------------------------------------
                # Continue - sleep 10 sec to let Azure Resource Graph pick up the new DCR
                #--------------------------------------------------------------------------

                    Write-Verbose ""
                    Write-Verbose "Waiting 10 sec to let Azure sync up so DCR rule can be retrieved from Azure Resource Graph"
                    Start-Sleep -Seconds 10

                #--------------------------------------------------------------------------
                # updating DCR list using Azure Resource Graph due to new DCR was created
                #--------------------------------------------------------------------------

                    $global:AzDcrDetails = Get-AzDcrListAll -AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId -Verbose:$Verbose

                #--------------------------------------------------------------------------
                # delegating Monitor Metrics Publisher Rolepermission to Log Ingest App
                #--------------------------------------------------------------------------

                    If ($AzDcrSetLogIngestApiAppPermissionsDcrLevel -eq $true)
                        {
                            $DcrRule = $global:AzDcrDetails | where-Object { $_.name -eq $DcrName }
                            $DcrRuleId = $DcrRule.id

                            Write-Verbose ""
                            Write-Verbose "Setting Monitor Metrics Publisher Role permissions on DCR [ $($DcrName) ]"

                            $guid = (new-guid).guid
                            $monitorMetricsPublisherRoleId = "3913510d-42f4-4e42-8a64-420c390055eb"
                            $roleDefinitionId = "/subscriptions/$($DcrSubscription)/providers/Microsoft.Authorization/roleDefinitions/$($monitorMetricsPublisherRoleId)"
                            $roleUrl = "https://management.azure.com" + $DcrRuleId + "/providers/Microsoft.Authorization/roleAssignments/$($Guid)?api-version=2018-07-01"
                            $roleBody = @{
                                properties = @{
                                    roleDefinitionId = $roleDefinitionId
                                    principalId      = $LogIngestServicePricipleObjectId
                                    scope            = $DcrRuleId
                                }
                            }
                            $jsonRoleBody = $roleBody | ConvertTo-Json -Depth 6

                            $result = try
                                {
                                    invoke-restmethod -UseBasicParsing -Uri $roleUrl -Method PUT -Body $jsonRoleBody -headers $Headers -ErrorAction SilentlyContinue
                                }
                            catch
                                {
                                }

                            $StatusCode = $result.StatusCode
                            If ($StatusCode -eq "204")
                                {
                                    Write-host "  SUCCESS - data uploaded to LogAnalytics"
                                }
                            ElseIf ($StatusCode -eq "RequestEntityTooLarge")
                                {
                                    Write-Error "  Error 513 - You are sending too large data - make the dataset smaller"
                                }
                            Else
                                {
                                    Write-Error $result
                                }

                            # Sleep 10 sec to let Azure sync up
                            Write-Verbose ""
                            Write-Verbose "Waiting 10 sec to let Azure sync up for permissions to replicate"
                            Start-Sleep -Seconds 10
                            Write-Verbose ""
                        }
        }

    #--------------------------------------------------------------------------
    # DCR was found - we will do either a MERGE or OVERWRITE
    #--------------------------------------------------------------------------
        ElseIf ( ($Dcr) -and ($SchemaMode -eq "Merge") )
            {

                $TableUrl = "https://management.azure.com" + $AzLogWorkspaceResourceId + "/tables/$($TableName)_CL?api-version=2021-12-01-preview"
                $TableStatus = Try
                                    {
                                        invoke-restmethod -UseBasicParsing -Uri $TableUrl -Method GET -Headers $Headers
                                    }
                               Catch
                                    {
                                    }


                If ($TableStatus)
                    {
                        $CurrentTableSchema = $TableStatus.properties.schema.columns
                        $AzureTableSchema   = $TableStatus.properties.schema.standardColumns
                    }

                # start by building new schema hash, based on existing schema in LogAnalytics custom log table
                    $SchemaArrayDCRFormatHash = @()
                    ForEach ($Property in $CurrentTableSchema)
                        {
                            $Name = $Property.name
                            $Type = $Property.type

                            # Add all properties except TimeGenerated as it only exist in tables - not DCRs
                            If ($Name -ne "TimeGenerated")
                                {
                                    $SchemaArrayDCRFormatHash += @{
                                                                    name        = $name
                                                                    type        = $type
                                                                  }
                                }
                        }
                
                # Add specific Azure column-names, if found as standard Azure columns (migrated from v1)
                $LAV1StandardColumns = @("Computer","RawData")
                ForEach ($Column in $LAV1StandardColumns)
                    {
                        If ( ($Column -notin $SchemaArrayDCRFormatHash.name) -and ($Column -in $AzureTableSchema.name) )
                            {
                                    $SchemaArrayDCRFormatHash += @{
                                                                    name        = $column
                                                                    type        = "string"
                                                                  }
                            }
                    }


                # get current DCR schema
                $DcrInfo = $global:AzDcrDetails | Where-Object { $_.name -eq $DcrName }

                $StreamDeclaration = 'Custom-' + $TableName + '_CL'
                $CurrentDcrSchema = $DcrInfo.properties.streamDeclarations.$StreamDeclaration.columns

                # enum $CurrentDcrSchema - and check if it exists in $SchemaArrayDCRFormatHash (coming from LogAnalytics)
                $UpdateDCR = $False
                ForEach ($Property in $SchemaArrayDCRFormatHash)
                    {
                        $Name = $Property.name
                        $Type = $Property.type

                        # Skip if name = TimeGenerated as it only exist in tables - not DCRs
                        If ($Name -ne "TimeGenerated")
                            {
                                $ChkDcrSchema = $CurrentDcrSchema | Where-Object { ($_.name -eq $Name) }
                                    If (!($ChkDcrSchema))
                                        {
                                            # DCR must be updated, changes was detected !
                                            $UpdateDCR = $true
                                        }
                             }
                    }

                    #--------------------------------------------------------------------------
                    # Merge: build full payload to create DCR for log ingest (api) to custom logs
                    #--------------------------------------------------------------------------
                        If ($UpdateDCR -eq $true)
                            {
                                $DcrObject = [pscustomobject][ordered]@{
                                                properties = @{
                                                                dataCollectionEndpointId = $DceResourceId
                                                                streamDeclarations = @{
                                                                                        $StreamName = @{
	  				                                                                                        columns = @(
                                                                                                                        $SchemaArrayDCRFormatHash
                                                                                                                       )
                                                                                                       }
                                                                                      }
                                                                destinations = @{
                                                                                    logAnalytics = @(
                                                                                                        @{ 
                                                                                                            workspaceResourceId = $AzLogWorkspaceResourceId
                                                                                                            workspaceId = $LogWorkspaceId
                                                                                                            name = $DcrLogWorkspaceName
                                                                                                         }
                                                                                                    ) 

                                                                                }
                                                                dataFlows = @(
                                                                                @{
                                                                                    streams = @(
                                                                                                    $StreamName
                                                                                               )
                                                                                    destinations = @(
                                                                                                        $DcrLogWorkspaceName
                                                                                                    )
                                                                                    transformKql = $KustoDefault
                                                                                    outputStream = $StreamName
                                                                                 }
                                                                             )
                                                                }
                                                location = $DceLocation
                                                name = $DcrName
                                                type = "Microsoft.Insights/dataCollectionRules"
                                            }

                            #--------------------------------------------------------------------------
                            # Update DCR using merged payload
                            #--------------------------------------------------------------------------

                                Write-Verbose ""
                                Write-Verbose "Merge: Updating DCR [ $($DcrName) ] with new properties in schema"
                                Write-Verbose $DcrResourceId

                                $DcrPayload = $DcrObject | ConvertTo-Json -Depth 20

                                $Uri = "https://management.azure.com" + "$DcrResourceId" + "?api-version=2022-06-01"
                                invoke-webrequest -UseBasicParsing -Uri $Uri -Method PUT -Body $DcrPayload -Headers $Headers
                    }
                }

    #--------------------------------------------------------------------------
    # DCR was NOT found - we are in Migrate mode
    #--------------------------------------------------------------------------
        ElseIf (!($Dcr) -and ($SchemaMode -eq "Migrate") )
            {
                $TableUrl = "https://management.azure.com" + $AzLogWorkspaceResourceId + "/tables/$($TableName)_CL?api-version=2021-12-01-preview"
                $TableStatus = Try
                                    {
                                        invoke-restmethod -UseBasicParsing -Uri $TableUrl -Method GET -Headers $Headers
                                    }
                               Catch
                                    {
                                    }


                If ($TableStatus)
                    {
                        $CurrentTableSchema = $TableStatus.properties.schema.columns
                        $AzureTableSchema   = $TableStatus.properties.schema.standardColumns
                    }

                # start by building new schema hash, based on existing schema in LogAnalytics custom log table
                    $SchemaArrayDCRFormatHash = @()
                    ForEach ($Property in $CurrentTableSchema)
                        {
                            $Name = $Property.name
                            $Type = $Property.type

                            # Add all properties except TimeGenerated as it only exist in tables - not DCRs
                            If ($Name -ne "TimeGenerated")
                                {
                                    $SchemaArrayDCRFormatHash += @{
                                                                    name        = $name
                                                                    type        = $type
                                                                  }
                                }
                        }
                
                # Add specific Azure column-names, if found as standard Azure columns (migrated from v1)
                $LAV1StandardColumns = @("Computer","RawData")
                ForEach ($Column in $LAV1StandardColumns)
                    {
                        If ( ($Column -notin $SchemaArrayDCRFormatHash.name) -and ($Column -in $AzureTableSchema.name) )
                            {
                                    $SchemaArrayDCRFormatHash += @{
                                                                    name        = $column
                                                                    type        = "string"
                                                                  }
                            }
                    }


                #--------------------------------------------------------------------------
                # build initial payload to create DCR for log ingest (api) to custom logs
                #--------------------------------------------------------------------------

                    If ($SchemaArrayDCRFormatHash.count -gt 10)
                        {
                            $SchemaSourceObjectLimited = $SchemaArrayDCRFormatHash[0..10]
                        }
                    Else
                        {
                            $SchemaSourceObjectLimited = $SchemaArrayDCRFormatHash
                        }


                    $DcrObject = [pscustomobject][ordered]@{
                                    properties = @{
                                                    dataCollectionEndpointId = $DceResourceId
                                                    streamDeclarations = @{
                                                                            $StreamName = @{
	  				                                                                            columns = @(
                                                                                                            $SchemaSourceObjectLimited
                                                                                                           )
                                                                                           }
                                                                          }
                                                    destinations = @{
                                                                        logAnalytics = @(
                                                                                            @{ 
                                                                                                workspaceResourceId = $AzLogWorkspaceResourceId
                                                                                                workspaceId = $LogWorkspaceId
                                                                                                name = $DcrLogWorkspaceName
                                                                                             }
                                                                                        ) 

                                                                    }
                                                    dataFlows = @(
                                                                    @{
                                                                        streams = @(
                                                                                        $StreamName
                                                                                   )
                                                                        destinations = @(
                                                                                            $DcrLogWorkspaceName
                                                                                        )
                                                                        transformKql = $KustoDefault
                                                                        outputStream = $StreamName
                                                                     }
                                                                 )
                                                    }
                                    location = $DceLocation
                                    name = $DcrName
                                    type = "Microsoft.Insights/dataCollectionRules"
                                }

                #--------------------------------------------------------------------------
                # create initial DCR using payload
                #--------------------------------------------------------------------------

                    Write-Verbose ""
                    Write-Verbose "Migration - Creating/updating DCR [ $($DcrName) ] with limited payload"
                    Write-Verbose $DcrResourceId

                    $DcrPayload = $DcrObject | ConvertTo-Json -Depth 20

                    $Uri = "https://management.azure.com" + "$DcrResourceId" + "?api-version=2022-06-01"
                    invoke-webrequest -UseBasicParsing -Uri $Uri -Method PUT -Body $DcrPayload -Headers $Headers
        
                    # sleeping to let API sync up before modifying
                    Start-Sleep -s 5

                #--------------------------------------------------------------------------
                # build full payload to create DCR for log ingest (api) to custom logs
                #--------------------------------------------------------------------------
                
                    $DcrObject = [pscustomobject][ordered]@{
                                    properties = @{
                                                    dataCollectionEndpointId = $DceResourceId
                                                    streamDeclarations = @{
                                                                            $StreamName = @{
	  				                                                                            columns = @(
                                                                                                            $SchemaArrayDCRFormatHash
                                                                                                           )
                                                                                           }
                                                                          }
                                                    destinations = @{
                                                                        logAnalytics = @(
                                                                                            @{ 
                                                                                                workspaceResourceId = $AzLogWorkspaceResourceId
                                                                                                workspaceId = $LogWorkspaceId
                                                                                                name = $DcrLogWorkspaceName
                                                                                             }
                                                                                        ) 

                                                                    }
                                                    dataFlows = @(
                                                                    @{
                                                                        streams = @(
                                                                                        $StreamName
                                                                                   )
                                                                        destinations = @(
                                                                                            $DcrLogWorkspaceName
                                                                                        )
                                                                        transformKql = $KustoDefault
                                                                        outputStream = $StreamName
                                                                     }
                                                                 )
                                                    }
                                    location = $DceLocation
                                    name = $DcrName
                                    type = "Microsoft.Insights/dataCollectionRules"
                                }

                #--------------------------------------------------------------------------
                # create DCR using payload
                #--------------------------------------------------------------------------

                    Write-Verbose ""
                    Write-Verbose "Migration - Updating DCR [ $($DcrName) ] with full payload"
                    Write-Verbose $DcrResourceId

                    $DcrPayload = $DcrObject | ConvertTo-Json -Depth 20

                    $Uri = "https://management.azure.com" + "$DcrResourceId" + "?api-version=2022-06-01"
                    invoke-webrequest -UseBasicParsing -Uri $Uri -Method PUT -Body $DcrPayload -Headers $Headers


                #--------------------------------------------------------------------------
                # Continue - sleep 10 sec to let Azure Resource Graph pick up the new DCR
                #--------------------------------------------------------------------------

                    Write-Verbose ""
                    Write-Verbose "Waiting 10 sec to let Azure sync up so DCR rule can be retrieved from Azure Resource Graph"
                    Start-Sleep -Seconds 10

                #--------------------------------------------------------------------------
                # updating DCR list using Azure Resource Graph due to new DCR was created
                #--------------------------------------------------------------------------

                    $global:AzDcrDetails = Get-AzDcrListAll -AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId -Verbose:$Verbose

                #--------------------------------------------------------------------------
                # delegating Monitor Metrics Publisher Rolepermission to Log Ingest App
                #--------------------------------------------------------------------------

                    If ($AzDcrSetLogIngestApiAppPermissionsDcrLevel -eq $true)
                        {
                            $DcrRule = $global:AzDcrDetails | where-Object { $_.name -eq $DcrName }
                            $DcrRuleId = $DcrRule.id

                            Write-Verbose ""
                            Write-Verbose "Setting Monitor Metrics Publisher Role permissions on DCR [ $($DcrName) ]"

                            $guid = (new-guid).guid
                            $monitorMetricsPublisherRoleId = "3913510d-42f4-4e42-8a64-420c390055eb"
                            $roleDefinitionId = "/subscriptions/$($DcrSubscription)/providers/Microsoft.Authorization/roleDefinitions/$($monitorMetricsPublisherRoleId)"
                            $roleUrl = "https://management.azure.com" + $DcrRuleId + "/providers/Microsoft.Authorization/roleAssignments/$($Guid)?api-version=2018-07-01"
                            $roleBody = @{
                                properties = @{
                                    roleDefinitionId = $roleDefinitionId
                                    principalId      = $LogIngestServicePricipleObjectId
                                    scope            = $DcrRuleId
                                }
                            }
                            $jsonRoleBody = $roleBody | ConvertTo-Json -Depth 6

                            $result = try
                                {
                                    invoke-restmethod -UseBasicParsing -Uri $roleUrl -Method PUT -Body $jsonRoleBody -headers $Headers -ErrorAction SilentlyContinue
                                }
                            catch
                                {
                                }

                            $StatusCode = $result.StatusCode
                            If ($StatusCode -eq "204")
                                {
                                    Write-host "  SUCCESS - data uploaded to LogAnalytics"
                                }
                            ElseIf ($StatusCode -eq "RequestEntityTooLarge")
                                {
                                    Write-Error "  Error 513 - You are sending too large data - make the dataset smaller"
                                }
                            Else
                                {
                                    Write-Error $result
                                }

                            # Sleep 10 sec to let Azure sync up
                            Write-Verbose ""
                            Write-Verbose "Waiting 10 sec to let Azure sync up for permissions to replicate"
                            Start-Sleep -Seconds 10
                            Write-Verbose ""
                        }
            }
}
