Function Get-AzAccessTokenManagement
{
    [CmdletBinding()]
    param(
            [Parameter()]
                [string]$AzAppId,
            [Parameter()]
                [string]$AzAppSecret,
            [Parameter()]
                [string]$TenantId
         )

        If ( ($AzAppId) -and ($AzAppSecret) -and ($TenantId) )
            {
                $AccessTokenUri = 'https://management.azure.com/'
                $oAuthUri       = "https://login.microsoftonline.com/$($TenantId)/oauth2/token"
                $authBody       = [Ordered] @{
                                               resource = "$AccessTokenUri"
                                               client_id = "$($AzAppId)"
                                               client_secret = "$($AzAppSecret)"
                                               grant_type = 'client_credentials'
                                             }
                $authResponse = Invoke-RestMethod -Method Post -Uri $oAuthUri -Body $authBody -ErrorAction Stop
                $token = $authResponse.access_token

                # Set the WebRequest headers
                $Headers = @{
                                'Content-Type' = 'application/json'
                                'Accept' = 'application/json'
                                'Authorization' = "Bearer $token"
                            }
            }
        Else
            {
                $AccessToken = Get-AzAccessToken -ResourceUrl https://management.azure.com/
                $Token = $AccessToken.Token

                $Headers = @{
                                'Content-Type' = 'application/json'
                                'Accept' = 'application/json'
                                'Authorization' = "Bearer $token"
                           }
            }

    Return $Headers
}

# Function CreateUpdate-AzLogAnalyticsCustomLogTableDcr ($TableName, $SchemaSourceObject, $AzLogWorkspaceResourceId, $AzAppId, $AzAppSecret, $TenantId)

Function CreateUpdate-AzLogAnalyticsCustomLogTableDcr
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [string]$TableName,
            [Parameter(mandatory)]
                [array]$SchemaSourceObject,
            [Parameter(mandatory)]
                [string]$AzLogWorkspaceResourceId,
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
                                               -TenantId $TenantId

    #--------------------------------------------------------------------------
    # LogAnalytics Table check
    #--------------------------------------------------------------------------

        $Table         = $TableName  + "_CL"    # TableName with _CL (CustomLog)

        If ($Table.Length -gt 45)
            {
                write-host "ERROR - Reduce length of tablename, as it has a maximum of 45 characters (current length: $($Table.Length))"
                pause
            }

    #--------------------------------------------------------------------------
    # Creating/Updating LogAnalytics Table based upon data source schema
    #--------------------------------------------------------------------------

        $Changes = $SchemaSourceObject[40]

<#
        $tableBodyPatch = @{
                                properties = @{
                                                schema = @{
                                                                name    = $Table
                                                                columns = @($Changes)
                                                            }
                                            }
                           } | ConvertTo-Json -Depth 10
#>
        $tableBodyPut   = @{
                                properties = @{
                                                schema = @{
                                                                name    = $Table
                                                                columns = @($SchemaSourceObject)
                                                            }
                                            }
                           } | ConvertTo-Json -Depth 10

        # create/update table schema using REST
        $TableUrl = "https://management.azure.com" + $AzLogWorkspaceResourceId + "/tables/$($Table)?api-version=2021-12-01-preview"

        Try
            {
                Write-Host ""
                Write-host "Trying to update existing LogAnalytics table schema for table [ $($Table) ] in "
                Write-host $AzLogWorkspaceResourceId

                Invoke-WebRequest -Uri $TableUrl -Method Patch -Headers $Headers -Body $TablebodyPut
            }
        Catch
            {
                Try
                    {
                        Write-Host ""
                        Write-Host "LogAnalytics Table doesn't exist or problems detected .... creating table [ $($Table) ] in"
                        Write-host $AzLogWorkspaceResourceId

                        Invoke-WebRequest -Uri $TableUrl -Method PUT -Headers $Headers -Body $TablebodyPut
                    }
                Catch
                    {
                        Write-Host ""
                        Write-Host "Something went wrong .... recreating table [ $($Table) ] in"
                        Write-host $AzLogWorkspaceResourceId

                        Invoke-WebRequest -Uri $TableUrl -Method DELETE -Headers $Headers
                                
                        Start-Sleep -Seconds 10
                                
                        Invoke-WebRequest -Uri $TableUrl -Method PUT -Headers $Headers -Body $TablebodyPut
                    }
            }
        
        return
}

<#
    Function CreateUpdate-AzDataCollectionRuleLogIngestCustomLog ($SchemaSourceObject, $AzLogWorkspaceResourceId, $DceName, $DcrName, $TableName, $TablePrefix, $AzDcrSetLogIngestApiAppPermissionsDcrLevel, `
                                                                  $LogIngestServicePricipleObjectId, $AzAppId, $AzAppSecret, $TenantId)
#>

Function CreateUpdate-AzDataCollectionRuleLogIngestCustomLog
{

    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [array]$SchemaSourceObject,
            [Parameter(mandatory)]
                [string]$AzLogWorkspaceResourceId,
            [Parameter(mandatory)]
                [string]$DceName,
            [Parameter(mandatory)]
                [string]$DcrName,
            [Parameter(mandatory)]
                [string]$TableName,
            [Parameter(mandatory)]
                [string]$AzDcrSetLogIngestApiAppPermissionsDcrLevel,
            [Parameter(mandatory)]
                [string]$LogIngestServicePricipleObjectId,
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
                                               -TenantId $TenantId

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
                        $ResponseRaw         = Invoke-WebRequest -Method POST -Uri $AzGraphUri -Headers $Headers -Body $AzGraphQuery
                        $ResponseData       += $ResponseRaw.content
                        $ResponseNextLink    = $ResponseRaw."@odata.nextLink"

                        While ($ResponseNextLink -ne $null)
                            {
                                $ResponseRaw         = Invoke-WebRequest -Method POST -Uri $AzGraphUri -Headers $Headers -Body $AzGraphQuery
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
        $LogWorkspaceId = (Invoke-RestMethod -Uri $LogWorkspaceUrl -Method GET -Headers $Headers).properties.customerId
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
        $DcrResourceGroup                           = "rg-dcr-" + $DcrLogWorkspaceName
        $DcrResourceId                              = "/subscriptions/$($DcrSubscription)/resourceGroups/$($DcrResourceGroup)/providers/microsoft.insights/dataCollectionRules/$($DcrName)"

    #--------------------------------------------------------------------------
    # Create resource group, if missing
    #--------------------------------------------------------------------------

        $Uri = "https://management.azure.com" + "/subscriptions/" + $DcrSubscription + "/resourcegroups/" + $DcrResourceGroup + "?api-version=2021-04-01"

        $CheckRG = Invoke-WebRequest -Uri $Uri -Method GET -Headers $Headers
        If ($CheckRG -eq $null)
            {
                $Body = @{
                            "location" = $DceLocation
                         } | ConvertTo-Json -Depth 5   

                Write-Host "Creating Resource group $($DcrResourceGroup) ... Please Wait !"
                $Uri = "https://management.azure.com" + "/subscriptions/" + $DcrSubscription + "/resourcegroups/" + $DcrResourceGroup + "?api-version=2021-04-01"
                $CreateRG = Invoke-WebRequest -Uri $Uri -Method PUT -Body $Body -Headers $Headers
            }

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

        Write-Host ""
        Write-host "Creating/updating DCR [ $($DcrName) ] with limited payload"
        Write-host $DcrResourceId

        $DcrPayload = $DcrObject | ConvertTo-Json -Depth 20

        $Uri = "https://management.azure.com" + "$DcrResourceId" + "?api-version=2022-06-01"
        Invoke-WebRequest -Uri $Uri -Method PUT -Body $DcrPayload -Headers $Headers
        
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

        Write-Host ""
        Write-host "Updating DCR [ $($DcrName) ] with full schema"
        Write-host $DcrResourceId

        $DcrPayload = $DcrObject | ConvertTo-Json -Depth 20

        $Uri = "https://management.azure.com" + "$DcrResourceId" + "?api-version=2022-06-01"
        Invoke-WebRequest -Uri $Uri -Method PUT -Body $DcrPayload -Headers $Headers

    #--------------------------------------------------------------------------
    # sleep 10 sec to let Azure Resource Graph pick up the new DCR
    #--------------------------------------------------------------------------

        Write-Host ""
        Write-host "Waiting 10 sec to let Azure sync up so DCR rule can be retrieved from Azure Resource Graph"
        Start-Sleep -Seconds 10

    #--------------------------------------------------------------------------
    # updating DCR list using Azure Resource Graph due to new DCR was created
    #--------------------------------------------------------------------------

        $global:AzDcrDetails = Get-AzDcrListAll -AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId

    #--------------------------------------------------------------------------
    # delegating Monitor Metrics Publisher Rolepermission to Log Ingest App
    #--------------------------------------------------------------------------

        If ($AzDcrSetLogIngestApiAppPermissionsDcrLevel -eq $true)
            {
                $DcrRule = $global:AzDcrDetails | where-Object { $_.name -eq $DcrName }
                $DcrRuleId = $DcrRule.id

                Write-Host ""
                Write-host "Setting Monitor Metrics Publisher Role permissions on DCR [ $($DcrName) ]"

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
                        Invoke-RestMethod -Uri $roleUrl -Method PUT -Body $jsonRoleBody -headers $Headers -ErrorAction SilentlyContinue
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
                        Write-Host "  Error 513 - You are sending too large data - make the dataset smaller"
                    }
                Else
                    {
                        Write-host $result
                    }

                # Sleep 10 sec to let Azure sync up
                Write-Host ""
                Write-host "Waiting 10 sec to let Azure sync up for permissions to replicate"
                Start-Sleep -Seconds 10
                Write-Host ""
            }

}

#Function Update-AzDataCollectionRuleResetTransformKqlDefault ($DcrResourceId, $AzAppId, $AzAppSecret, $TenantId)
           
Function Update-AzDataCollectionRuleResetTransformKqlDefault
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [string]$DcrResourceId,
            [Parameter()]
                [string]$AzAppId,
            [Parameter()]
                [string]$AzAppSecret,
            [Parameter()]
                [string]$TenantId
         )

    #--------------------------------------------------------------------------
    # Variables
    #--------------------------------------------------------------------------

        $DefaultTransformKqlDcrLogIngestCustomLog = "source | extend TimeGenerated = now()"

    #--------------------------------------------------------------------------
    # Connection
    #--------------------------------------------------------------------------

        $Headers = Get-AzAccessTokenManagement -AzAppId $AzAppId `
                                               -AzAppSecret $AzAppSecret `
                                               -TenantId $TenantId

    #--------------------------------------------------------------------------
    # get existing DCR
    #--------------------------------------------------------------------------

        $DcrUri = "https://management.azure.com" + $DcrResourceId + "?api-version=2022-06-01"
        $DCR = Invoke-RestMethod -Uri $DcrUri -Method GET -Headers $Headers
        $DcrObj = $DCR.Content | ConvertFrom-Json

    #--------------------------------------------------------------------------
    # update payload object
    #--------------------------------------------------------------------------

        $DCRObj.properties.dataFlows[0].transformKql = $DefaultTransformKqlDcrLogIngestCustomLog

    #--------------------------------------------------------------------------
    # update existing DCR
    #--------------------------------------------------------------------------

        Write-host "  Resetting transformKql to default for DCR"
        Write-host $DcrResourceId

        # convert modified payload to JSON-format
        $DcrPayload = $DcrObj | ConvertTo-Json -Depth 20

        # update changes to existing DCR
        $DcrUri = "https://management.azure.com" + $DcrResourceId + "?api-version=2022-06-01"
        $DCR = Invoke-RestMethod -Uri $DcrUri -Method PUT -Body $DcrPayload -Headers $Headers
}

# Function Update-AzDataCollectionRuleTransformKql ($DcrResourceId, $transformKql, $AzAppId, $AzAppSecret, $TenantId)

Function Update-AzDataCollectionRuleTransformKql
{

    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [string]$DcrResourceId,
            [Parameter(mandatory)]
                [string]$transformKql,
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
                                               -TenantId $TenantId

    #--------------------------------------------------------------------------
    # get existing DCR
    #--------------------------------------------------------------------------

        $DcrUri = "https://management.azure.com" + $DcrResourceId + "?api-version=2022-06-01"
        $DCR = Invoke-RestMethod -Uri $DcrUri -Method GET -Headers $Headers

    #--------------------------------------------------------------------------
    # update payload object
    #--------------------------------------------------------------------------

        If ($DCR.properties.dataFlows[0].transformKql)
            {
                # changing value on existing property
                $DCR.properties.dataFlows[0].transformKql = $transformKql
            }
        Else
            {
                # Adding new property to object
                $DCR.properties.dataFlows[0] | Add-Member -NotePropertyName transformKql -NotePropertyValue $transformKql -Force
            }


    #--------------------------------------------------------------------------
    # update existing DCR
    #--------------------------------------------------------------------------

        Write-host "Updating transformKql for DCR"
        Write-host $DcrResourceId

        # convert modified payload to JSON-format
        $DcrPayload = $Dcr | ConvertTo-Json -Depth 20

        # update changes to existing DCR
        $DcrUri = "https://management.azure.com" + $DcrResourceId + "?api-version=2022-06-01"
        $DCR = Invoke-RestMethod -Uri $DcrUri -Method PUT -Body $DcrPayload -Headers $Headers
}

# Function Update-AzDataCollectionRuleLogAnalyticsCustomLogTableSchema ($SchemaSourceObject, $TableName, $DcrResourceId, $AzLogWorkspaceResourceId, $AzAppId, $AzAppSecret, $TenantId)

Function Update-AzDataCollectionRuleLogAnalyticsCustomLogTableSchema
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [hashtable]$SchemaSourceObject,
            [Parameter(mandatory)]
                [string]$TableName,
            [Parameter(mandatory)]
                [string]$DcrResourceId,
            [Parameter(mandatory)]
                [string]$AzLogWorkspaceResourceId,
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
                                               -TenantId $TenantId

    #--------------------------------------------------------------------------
    # build LogAnalytics Table schema based upon data source
    #--------------------------------------------------------------------------

        $Table         = $TableName  + "_CL"    # TableName with _CL (CustomLog)

        # Build initial hash used for columns for table schema
        $TableSchemaHash = @()

        # Requirement - Add TimeGenerated to array
        $TableSchemaObjHash = @{
                                    name        = "TimeGenerated"
                                    type        = "datetime"
                                    description = ""
                               }
        $TableSchemaHash    += $TableSchemaObjHash

        # Loop source object and build hash for table schema
        $ObjColumns = $SchemaSourceObject[0] | ConvertTo-Json -Depth 100 | ConvertFrom-Json | Get-Member -MemberType NoteProperty
        ForEach ($Column in $ObjColumns)
            {
                $ObjDefinitionStr = $Column.Definition
                        If ($ObjDefinitionStr -like "int*")                                            { $ObjType = "int" }
                    ElseIf ($ObjDefinitionStr -like "real*")                                           { $ObjType = "int" }
                    ElseIf ($ObjDefinitionStr -like "long*")                                           { $ObjType = "long" }
                    ElseIf ($ObjDefinitionStr -like "guid*")                                           { $ObjType = "dynamic" }
                    ElseIf ($ObjDefinitionStr -like "string*")                                         { $ObjType = "string" }
                    ElseIf ($ObjDefinitionStr -like "datetime*")                                       { $ObjType = "datetime" }
                    ElseIf ($ObjDefinitionStr -like "bool*")                                           { $ObjType = "boolean" }
                    ElseIf ($ObjDefinitionStr -like "object*")                                         { $ObjType = "dynamic" }
                    ElseIf ($ObjDefinitionStr -like "System.Management.Automation.PSCustomObject*")    { $ObjType = "dynamic" }

                $TableSchemaObjHash = @{
                                            name        = $Column.Name
                                            type        = $ObjType
                                            description = ""
                                        }
                $TableSchemaHash    += $TableSchemaObjHash
            }

        # build table schema
        $tableBody = @{
                            properties = @{
                                            schema = @{
                                                            name    = $Table
                                                            columns = $TableSchemaHash
                                                        }
                                        }
                      } | ConvertTo-Json -Depth 10


    #--------------------------------------------------------------------------
    # update existing LogAnalytics Table based upon data source schema
    #--------------------------------------------------------------------------

        Write-host "  Updating LogAnalytics table schema for table [ $($Table) ]"
        Write-host ""

        # create/update table schema using REST
        $TableUrl = "https://management.azure.com" + $AzLogWorkspaceResourceId + "/tables/$($Table)?api-version=2021-12-01-preview"
        Invoke-RestMethod -Uri $TableUrl -Method PUT -Headers $Headers -Body $Tablebody

    #--------------------------------------------------------------------------
    # build Dcr schema based upon data source
    #--------------------------------------------------------------------------

        $DcrObjColumns = $SchemaSourceObject[0] | ConvertTo-Json -Depth 100 | ConvertFrom-Json | Get-Member -MemberType NoteProperty
        
        $TableSchemaObject = @()

        # Requirement - Add TimeGenerated to array
        $TableSchemaObj = @{
                                    name        = "TimeGenerated"
                                    type        = "datetime"
                               }
        $TableSchemaObject   += $TableSchemaObj

        
        ForEach ($Column in $DcrObjColumns)
            {
                $ObjDefinitionStr = $Column.Definition
                        If ($ObjDefinitionStr -like "int*")                                            { $ObjType = "int" }
                    ElseIf ($ObjDefinitionStr -like "real*")                                           { $ObjType = "int" }
                    ElseIf ($ObjDefinitionStr -like "long*")                                           { $ObjType = "long" }
                    ElseIf ($ObjDefinitionStr -like "guid*")                                           { $ObjType = "dynamic" }
                    ElseIf ($ObjDefinitionStr -like "string*")                                         { $ObjType = "string" }
                    ElseIf ($ObjDefinitionStr -like "datetime*")                                       { $ObjType = "datetime" }
                    ElseIf ($ObjDefinitionStr -like "bool*")                                           { $ObjType = "boolean" }
                    ElseIf ($ObjDefinitionStr -like "object*")                                         { $ObjType = "dynamic" }
                    ElseIf ($ObjDefinitionStr -like "System.Management.Automation.PSCustomObject*")    { $ObjType = "dynamic" }

                $TableSchemaObj = @{
                                        "name"         = $Column.Name
                                        "type"         = $ObjType
                                    }
                $TableSchemaObject    += $TableSchemaObj
            }

    #--------------------------------------------------------------------------
    # get existing DCR
    #--------------------------------------------------------------------------

        $DcrUri = "https://management.azure.com" + $DcrResourceId + "?api-version=2022-06-01"
        $DCR = Invoke-RestMethod -Uri $DcrUri -Method GET
        $DcrObj = $DCR.Content | ConvertFrom-Json

    #--------------------------------------------------------------------------
    # update schema declaration in Dcr payload object
    #--------------------------------------------------------------------------

        $StreamName = "Custom-" + $TableName + "_CL"
        $DcrObj.properties.streamDeclarations.$StreamName.columns = $TableSchemaObject

    #--------------------------------------------------------------------------
    # update existing DCR
    #--------------------------------------------------------------------------

        # convert modified payload to JSON-format
        $DcrPayload = $DcrObj | ConvertTo-Json -Depth 20

        Write-host "  Updating declaration schema [ $($StreamName) ] for DCR"
        Write-host $DcrResourceId

        # update changes to existing DCR
        $DcrUri = "https://management.azure.com" + $DcrResourceId + "?api-version=2022-06-01"
        $DCR = Invoke-RestMethod -Uri $DcrUri -Method PUT -Body $DcrPayload -Headers $Headers
}

#Function Update-AzDataCollectionRuleDceEndpoint ($DcrResourceId, $DceResourceId, $AzAppId, $AzAppSecret, $TenantId)

Function Update-AzDataCollectionRuleDceEndpoint
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [string]$DcrResourceId,
            [Parameter(mandatory)]
                [string]$DceResourceId,
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
                                               -TenantId $TenantId

    #--------------------------------------------------------------------------
    # get existing DCR
    #--------------------------------------------------------------------------

        $DcrUri = "https://management.azure.com" + $DcrResourceId + "?api-version=2022-06-01"
        $DCR = Invoke-RestMethod -Uri $DcrUri -Method GET -Headers $headers

    #--------------------------------------------------------------------------
    # update payload object
    #--------------------------------------------------------------------------

        $DCR.properties.dataCollectionEndpointId = $DceResourceId

    #--------------------------------------------------------------------------
    # update existing DCR
    #--------------------------------------------------------------------------

        Write-host "Updating DCE EndpointId for DCR"
        Write-host $DcrResourceId

        # convert modified payload to JSON-format
        $DcrPayload = $Dcr | ConvertTo-Json -Depth 20

        # update changes to existing DCR
        $DcrUri = "https://management.azure.com" + $DcrResourceId + "?api-version=2022-06-01"
        $DCR = Invoke-RestMethod -Uri $DcrUri -Method PUT -Body $DcrPayload -Headers $Headers
}

#Function Delete-AzLogAnalyticsCustomLogTables ($TableNameLike, $AzLogWorkspaceResourceId, $AzAppId, $AzAppSecret, $TenantId)

Function Delete-AzLogAnalyticsCustomLogTables
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [string]$TableNameLike,
            [Parameter(mandatory)]
                [string]$AzLogWorkspaceResourceId,
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
                                               -TenantId $TenantId


    #--------------------------------------------------------------------------
    # Getting list of Azure LogAnalytics tables
    #--------------------------------------------------------------------------

        Write-host "Getting list of tables in "
        Write-host $AzLogWorkspaceResourceId

        # create/update table schema using REST
        $TableUrl   = "https://management.azure.com" + $AzLogWorkspaceResourceId + "/tables?api-version=2021-12-01-preview"
        $TablesRaw  = Invoke-RestMethod -Uri $TableUrl -Method GET -Headers $Headers
        $Tables     = $TablesRaw.value


    #--------------------------------------------------------------------------
    # Building list of tables to delete
    #--------------------------------------------------------------------------

        # custom Logs only
        $TablesScope = $Tables | where-object { $_.properties.schema.tableType -eq "CustomLog" }
        $TablesScope = $TablesScope  | where-object { $_.properties.schema.name -like $TableNameLike }

    #--------------------------------------------------------------------------
    # Deleting tables
    #--------------------------------------------------------------------------

        If ($TablesScope)
            {
                Write-host "LogAnalytics Resource Id"
                Write-host $AzLogWorkspaceResourceId
                Write-host ""
                Write-host "Table deletions in scope:"
                $TablesScope.properties.schema.name

                $yes = New-Object System.Management.Automation.Host.ChoiceDescription "&Yes","Delete"
                $no = New-Object System.Management.Automation.Host.ChoiceDescription "&No","Cancel"
                $options = [System.Management.Automation.Host.ChoiceDescription[]]($yes, $no)
                $heading = "Delete Azure Loganalytics tables"
                $message = "Do you want to continue with the deletion of the shown tables?"
                $Prompt = $host.ui.PromptForChoice($heading, $message, $options, 1)
                switch ($prompt) {
                                    0
                                        {
                                            ForEach ($TableInfo in $TablesScope)
                                                { 
                                                    $Table = $TableInfo.properties.schema.name
                                                    Write-host "Deleting LogAnalytics table [ $($Table) ] ... Please Wait !"

                                                    $TableUrl = "https://management.azure.com" + $AzLogWorkspaceResourceId + "/tables/$($Table)?api-version=2021-12-01-preview"
                                                    Invoke-RestMethod -Uri $TableUrl -Method DELETE -Headers $Headers
                                                }
                                        }
                                    1
                                        {
                                            Write-Host "No" -ForegroundColor Red
                                        }
                                }
            }
}

# Function Delete-AzDataCollectionRules ($DcrNameLike, $AzAppId, $AzAppSecret, $TenantId)

Function Delete-AzDataCollectionRules
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [string]$DcrNameLike,
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
                                               -TenantId $TenantId

    #--------------------------------------------------------------------------
    # Getting list of Azure Data Collection Rules using ARG
    #--------------------------------------------------------------------------

        $DCR_Rules_All = @()
        $pageSize = 1000
        $iteration = 0
        $searchParams = @{
                            Query = "Resources `
                                    | where type =~ 'microsoft.insights/datacollectionrules' "
                            First = $pageSize
                            }

        $results = do {
            $iteration += 1
            $pageResults = Search-AzGraph -UseTenantScope @searchParams
            $searchParams.Skip += $pageResults.Count
            $DCR_Rules_All += $pageResults
        } while ($pageResults.Count -eq $pageSize)

    #--------------------------------------------------------------------------
    # Building list of DCRs to delete
    #--------------------------------------------------------------------------

        $DcrScope = $DCR_Rules_All | Where-Object { $_.name -like $DcrNameLike }

    #--------------------------------------------------------------------------
    # Deleting DCRs
    #--------------------------------------------------------------------------

        If ($DcrScope)
            {
                Write-host "Data Collection Rules deletions in scope:"
                $DcrScope.name

                $yes = New-Object System.Management.Automation.Host.ChoiceDescription "&Yes","Delete"
                $no = New-Object System.Management.Automation.Host.ChoiceDescription "&No","Cancel"
                $options = [System.Management.Automation.Host.ChoiceDescription[]]($yes, $no)
                $heading = "Delete Azure Data Collection Rules"
                $message = "Do you want to continue with the deletion of the shown data collection rules?"
                $Prompt = $host.ui.PromptForChoice($heading, $message, $options, 1)
                switch ($prompt) {
                                    0
                                        {
                                            ForEach ($DcrInfo in $DcrScope)
                                                { 
                                                    $DcrResourceId = $DcrInfo.id
                                                    Write-host "Deleting Data Collection Rules [ $($DcrInfo.name) ] ... Please Wait !"
                                                    Invoke-AzRestMethod -Path ("$DcrResourceId"+"?api-version=2022-06-01") -Method DELETE
                                                }
                                        }
                                    1
                                        {
                                            Write-Host "No" -ForegroundColor Red
                                        }
                                }
            }
}

# Function Get-AzDcrDceDetails ($DceName, $DcrName, $AzAppId, $AzAppSecret, $TenantId)

Function Get-AzDcrDceDetails
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [string]$DceName,
            [Parameter(mandatory)]
                [string]$DcrName,
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
                                               -TenantId $TenantId

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
                                    # record not found - rebuild list and try again
                                    
                                    Start-Sleep -s 10

                                    # building global variable with all DCEs, which can be viewed by Log Ingestion app
                                    $global:AzDceDetails = Get-AzDceListAll -AzAppId $LogIngestAppId -AzAppSecret $LogIngestAppSecret -TenantId $TenantId
    
                                    $DceInfo = $global:AzDceDetails | Where-Object { $_.name -eq $DceName }
                                       If (!($DceInfo))
                                        {
                                            Write-Output "Could not find DCE with name [ $($DceName) ]"
                                        }
                                }
                    }
                Else
                    {
                        $AzGraphQuery = @{
                                            'query' = 'Resources | where type =~ "microsoft.insights/datacollectionendpoints" '
                                         } | ConvertTo-Json -Depth 20

                        $ResponseData = @()

                        $AzGraphUri          = "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2021-03-01"
                        $ResponseRaw         = Invoke-WebRequest -Method POST -Uri $AzGraphUri -Headers $Headers -Body $AzGraphQuery
                        $ResponseData       += $ResponseRaw.content
                        $ResponseNextLink    = $ResponseRaw."@odata.nextLink"

                        While ($ResponseNextLink -ne $null)
                            {
                                $ResponseRaw         = Invoke-WebRequest -Method POST -Uri $AzGraphUri -Headers $Headers -Body $AzGraphQuery
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

    #--------------------------------------------------------------------------
    # Get DCRs from Azure Resource Graph
    #--------------------------------------------------------------------------

        If ($DcrName)
            {
                If ($global:AzDcrDetails)   # global variables was defined. Used to mitigate throttling in Azure Resource Graph (free service)
                    {
                        # Retrieve DCE in scope
                        $DcrInfo = $global:AzDcrDetails | Where-Object { $_.name -eq $DcrName }
                            If (!($DcrInfo))
                                {
                                    # record not found - rebuild list and try again
                                    
                                    Start-Sleep -s 10

                                    # building global variable with all DCEs, which can be viewed by Log Ingestion app
                                    $global:AzDcrDetails = Get-AzDcrListAll -AzAppId $LogIngestAppId -AzAppSecret $LogIngestAppSecret -TenantId $TenantId
    
                                    $DcrInfo = $global:AzDceDetails | Where-Object { $_.name -eq $DcrName }
                                       If (!($DcInfo))
                                        {
                                            Write-Output "Could not find DCR with name [ $($DcrName) ]"
                                        }
                                }
                    }
                Else
                    {
                        $AzGraphQuery = @{
                                            'query' = 'Resources | where type =~ "microsoft.insights/datacollectionrules" '
                                         } | ConvertTo-Json -Depth 20

                        $ResponseData = @()

                        $AzGraphUri          = "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2021-03-01"
                        $ResponseRaw         = Invoke-WebRequest -Method POST -Uri $AzGraphUri -Headers $Headers -Body $AzGraphQuery
                        $ResponseData       += $ResponseRaw.content
                        $ResponseNextLink    = $ResponseRaw."@odata.nextLink"

                        While ($ResponseNextLink -ne $null)
                            {
                                $ResponseRaw         = Invoke-WebRequest -Method POST -Uri $AzGraphUri -Headers $Headers -Body $AzGraphQuery
                                $ResponseData       += $ResponseRaw.content
                                $ResponseNextLink    = $ResponseRaw."@odata.nextLink"
                            }
                        $DataJson = $ResponseData | ConvertFrom-Json
                        $Data     = $DataJson.data

                        $DcrInfo = $Data | Where-Object { $_.name -eq $DcrName }
                            If (!($DcrInfo))
                                {
                                    Write-Output "Could not find DCR with name [ $($DcrName) ]"
                                }
                    }
            }

    #--------------------------------------------------------------------------
    # values
    #--------------------------------------------------------------------------
        If ( ($DceName) -and ($DceInfo) )
            {
                $DceResourceId                                  = $DceInfo.id
                $DceLocation                                    = $DceInfo.location
                $DceURI                                         = $DceInfo.properties.logsIngestion.endpoint
                $DceImmutableId                                 = $DceInfo.properties.immutableId

                # return / output
                $DceResourceId
                $DceLocation
                $DceURI
                $DceImmutableId
            }

        If ( ($DcrName) -and ($DcrInfo) )
            {
                $DcrResourceId                                  = $DcrInfo.id
                $DcrLocation                                    = $DcrInfo.location
                $DcrImmutableId                                 = $DcrInfo.properties.immutableId
                $DcrStream                                      = $DcrInfo.properties.dataflows.outputStream
                $DcrDestinationsLogAnalyticsWorkSpaceName       = $DcrInfo.properties.destinations.logAnalytics.name
                $DcrDestinationsLogAnalyticsWorkSpaceId         = $DcrInfo.properties.destinations.logAnalytics.workspaceId
                $DcrDestinationsLogAnalyticsWorkSpaceResourceId = $DcrInfo.properties.destinations.logAnalytics.workspaceResourceId
                $DcrTransformKql                                = $DcrInfo.properties.dataFlows[0].transformKql


                # return / output
                $DcrResourceId
                $DcrLocation
                $DcrImmutableId
                $DcrStream
                $DcrDestinationsLogAnalyticsWorkSpaceName
                $DcrDestinationsLogAnalyticsWorkSpaceId
                $DcrDestinationsLogAnalyticsWorkSpaceResourceId
                $DcrTransformKql
            }

        return
}

#Function Post-AzLogAnalyticsLogIngestCustomLogDcrDce ($DceURI, $DcrImmutableId, $DcrStream, $Data, $BatchAmount, $AzAppId, $AzAppSecret, $TenantId)

Function Post-AzLogAnalyticsLogIngestCustomLogDcrDce
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [string]$DceURI,
            [Parameter(mandatory)]
                [string]$DcrImmutableId,
            [Parameter(mandatory)]
                [string]$DcrStream,
            [Parameter(mandatory)]
                [Array]$Data,
            [Parameter()]
                [string]$BatchAmount,
            [Parameter()]
                [string]$AzAppId,
            [Parameter()]
                [string]$AzAppSecret,
            [Parameter()]
                [string]$TenantId
         )

    #--------------------------------------------------------------------------
    # Data check
    #--------------------------------------------------------------------------
        If ($DceURI -and $DcrImmutableId -and $DcrStream -and $Data)
            {
                # Add assembly to upload using http
                Add-Type -AssemblyName System.Web

                #--------------------------------------------------------------------------
                # Obtain a bearer token used to authenticate against the data collection endpoint using Azure App & Secret
                #--------------------------------------------------------------------------

                    $scope       = [System.Web.HttpUtility]::UrlEncode("https://monitor.azure.com//.default")   
                    $bodytoken   = "client_id=$AzAppId&scope=$scope&client_secret=$AzAppSecret&grant_type=client_credentials";
                    $headers     = @{"Content-Type"="application/x-www-form-urlencoded"};
                    $uri         = "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token"
                    $bearerToken = (Invoke-RestMethod -Uri $uri -Method "Post" -Body $bodytoken -Headers $headers).access_token

                    $headers = @{
                                    "Authorization" = "Bearer $bearerToken";
                                    "Content-Type" = "application/json";
                                }

                #--------------------------------------------------------------------------
                # Upload the data using Log Ingesion API using DCE/DCR
                #--------------------------------------------------------------------------
                    
                    # initial variable
                    $indexLoopFrom = 0

                    # calculate size of data (entries)
                    $TotalDataLines = ($Data | Measure-Object).count

                    # calculate number of entries to send during each transfer - log ingestion api limits to max 1 mb per transfer
                    If ( ($TotalDataLines -gt 1) -and ($BatchAmount -eq $null) )
                        {
                            $SizeDataSingleEntryJson  = (ConvertTo-Json -Depth 100 -InputObject @($Data[0]) -Compress).length
                            $DataSendAmountDecimal    = (( 1mb - 300Kb) / $SizeDataSingleEntryJson)   # 500 Kb is overhead (my experience !)
                            $DataSendAmount           = [math]::Floor($DataSendAmountDecimal)
                        }
                    ElseIf ($BatchAmount)
                        {
                            $DataSendAmount           = $BatchAmount
                        }
                    Else
                        {
                            $DataSendAmount           = 1
                        }

                    # loop - upload data in batches, depending on possible size & Azure limits 
                    Do
                        {
                            $DataSendRemaining = $TotalDataLines - $indexLoopFrom

                            If ($DataSendRemaining -le $DataSendAmount)
                                {
                                    # send last batch - or whole batch
                                    $indexLoopTo    = $TotalDataLines - 1   # cause we start at 0 (zero) as first record
                                    $DataScopedSize = $Data   # no need to split up in batches
                                }
                            ElseIf ($DataSendRemaining -gt $DataSendAmount)
                                {
                                    # data must be splitted in batches
                                    $indexLoopTo    = $indexLoopFrom + $DataSendAmount
                                    $DataScopedSize = $Data[$indexLoopFrom..$indexLoopTo]
                                }

                            # Convert data into JSON-format
                            $JSON = ConvertTo-Json -Depth 100 -InputObject @($DataScopedSize) -Compress

                            If ($DataSendRemaining -gt 1)    # batch
                                {
                                    write-Output ""
                                    
                                    # we are showing as first record is 1, but actually is is in record 0 - but we change it for gui purpose
                                    Write-Output "  [ $($indexLoopFrom + 1)..$($indexLoopTo + 1) / $($TotalDataLines) ] - Posting data to Loganalytics table [ $($TableName)_CL ] .... Please Wait !"
                                }
                            ElseIf ($DataSendRemaining -eq 1)   # single record
                                {
                                    write-Output ""
                                    Write-Output "  [ $($indexLoopFrom + 1) / $($TotalDataLines) ] - Posting data to Loganalytics table [ $($TableName)_CL ] .... Please Wait !"
                                }

                            $uri = "$DceURI/dataCollectionRules/$DcrImmutableId/streams/$DcrStream"+"?api-version=2021-11-01-preview"
                            
                            # set encoding to UTF8
                            $JSON = [System.Text.Encoding]::UTF8.GetBytes($JSON)

                            $Result = Invoke-WebRequest -Uri $uri -Method POST -Body $JSON -Headers $headers -ErrorAction SilentlyContinue
                            $StatusCode = $Result.StatusCode

                            If ($StatusCode -eq "204")
                                {
                                    Write-host "  SUCCESS - data uploaded to LogAnalytics"
                                }
                            ElseIf ($StatusCode -eq "RequestEntityTooLarge")
                                {
                                    Write-Host "  Error 513 - You are sending too large data - make the dataset smaller"
                                }
                            Else
                                {
                                    Write-host $result
                                }

                            # Set new Fom number, based on last record sent
                            $indexLoopFrom = $indexLoopTo

                        }
                    Until ($IndexLoopTo -ge ($TotalDataLines - 1 ))
            
              # return $result
        }
        Write-host ""
}

#Function ValidateFix-AzLogAnalyticsTableSchemaColumnNames ($Data)

Function ValidateFix-AzLogAnalyticsTableSchemaColumnNames
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data
         )

    $ProhibitedColumnNames = @("_ResourceId","id","_ResourceId","_SubscriptionId","TenantId","Type","UniqueId","Title")

    Write-host "  Validating schema structure of source data ... Please Wait !"

    #-----------------------------------------------------------------------    
    # Initial check
    $IssuesFound = $false

    $data = $DataVariable

        # loop through data
        ForEach ($Entry in $Data)
            {
                $ObjColumns = $Entry | Get-Member -MemberType NoteProperty

                ForEach ($Column in $ObjColumns)
                    {
                        # get column name
                        $ColumnName = $Column.Name

                        If ($ColumnName -in $ProhibitedColumnNames)   # prohibited column names
                            {
                                $IssuesFound = $true
                                write-host "  ISSUE - Column name is prohibited [ $($ColumnName) ]"
                            }

                        ElseIf ($ColumnName -like "_*")   # remove any leading underscores - column in DCR/LA must start with a character
                            {
                                $IssuesFound = $true
                                write-host "  ISSUE - Column name must start with character [ $($ColumnName) ]"
                            }
                        ElseIf ($ColumnName -like "*.*")   # includes . (period)
                            {
                                $IssuesFound = $true
                                write-host "  ISSUE - Column name include . (period) - must be removed [ $($ColumnName) ]"
                            }
                        ElseIf ($ColumnName -like "* *")   # includes whitespace " "
                            {
                                $IssuesFound = $true
                                write-host "  ISSUE - Column name include whitespace - must be removed [ $($ColumnName) ]"
                            }
                        ElseIf ($ColumnName.Length -gt 45)   # trim the length to maximum 45 characters
                            {
                                $IssuesFound = $true
                                write-host "  ISSUE - Column length is greater than 45 characters (trimming column name is neccessary)  [ $($ColumnName) ]"
                            }
                    }
            }

    If ($IssuesFound)
        {
            Write-host "  Issues found .... fixing schema structure of source data ... Please Wait !"

            $DataCount  = ($Data | Measure-Object).Count

            $DataVariableQA = @()

            $Data | ForEach-Object -Begin  {
                    $i = 0
            } -Process {

                    # get column names
                    $ObjColumns = $_ | Get-Member -MemberType NoteProperty

                    ForEach ($Column in $ObjColumns)
                        {
                            # get column name
                            $ColumnName = $Column.Name

                            If ($ColumnName -in $ProhibitedColumnNames)   # phohibited column names
                                {
                                    $UpdColumn  = $ColumnName + "_"
                                    $ColumnData = $_.$ColumnName
                                    $_ | Add-Member -MemberType NoteProperty -Name $UpdColumn -Value $ColumnData -Force
                                    $_.PSObject.Properties.Remove($ColumnName)
                                }
                            ElseIf ($ColumnName -like "*.*")   # remove any . (period)
                                {
                                    $UpdColumn = $ColumnName.Replace(".","")
                                    $ColumnData = $Entry.$Column
                                    $_ | Add-Member -MemberType NoteProperty -Name $UpdColumn -Value $ColumnData -Force
                                    $_.PSObject.Properties.Remove($ColumnName)
                                }
                            ElseIf ($ColumnName -like "_*")   # remove any leading underscores - column in DCR/LA must start with a character
                                {
                                    $UpdColumn = $ColumnName.TrimStart("_")
                                    $ColumnData = $Entry.$Column
                                    $_ | Add-Member -MemberType NoteProperty -Name $UpdColumn -Value $ColumnData -Force
                                    $_.PSObject.Properties.Remove($ColumnName)
                                }
                            ElseIf ($ColumnName -like "* *")   # remove any whitespaces
                                {
                                    $UpdColumn = $ColumnName.TrimStart()
                                    $ColumnData = $Entry.$Column
                                    $_ | Add-Member -MemberType NoteProperty -Name $UpdColumn -Value $ColumnData -Force
                                    $_.PSObject.Properties.Remove($ColumnName)
                                }
                            ElseIf ($ColumnName.Length -gt 45)   # trim the length to maximum 45 characters
                                {
                                    $UpdColumn = $ColumnName.Substring(0,45)
                                    $ColumnData = $_.$Column
                                    $_ | Add-Member -MemberType NoteProperty -Name $UpdColumn -Value $ColumnData -Force
                                    $_.PSObject.Properties.Remove($ColumnName)
                                }
                            Else    # write column name and data (OK)
                                {
                                    $ColumnData = $_.$ColumnName
                                    $_ | Add-Member -MemberType NoteProperty -Name $ColumnName -Value $ColumnData -Force
                                }
                        }
                    $DataVariableQA += $_

                    # Increment the $i counter variable which is used to create the progress bar.
                    $i = $i+1

                    # Determine the completion percentage
                    $Completed = ($i/$DataCount) * 100
                    Write-Progress -Activity "Validating/fixing schema structure of source object" -Status "Progress:" -PercentComplete $Completed
            } -End {
                $Data = $DataVariableQA
            }
        }
    Else
        {
            Write-host "  SUCCESS - No issues found in schema structure"
        }
    Return $Data
}

#Function Build-DataArrayToAlignWithSchema ($Data)

Function Build-DataArrayToAlignWithSchema
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data
         )

    Write-host "  Aligning source object structure with schema ... Please Wait !"
    
    # Get schema
    $Schema = Get-ObjectSchemaAsArray -Data $Data

    $DataCount  = ($Data | Measure-Object).Count

    $DataVariableQA = @()

    $Data | ForEach-Object -Begin  {
            $i = 0
    } -Process {
                    # get column names
                  #  $ObjColumns = $_ | Get-Member -MemberType NoteProperty

                    # enum schema
                    ForEach ($Column in $Schema)
                        {
                            # get column name & data
                            $ColumnName = $Column.Name
                            $ColumnData = $_.$ColumnName

                            $_ | Add-Member -MemberType NoteProperty -Name $ColumnName -Value $ColumnData -Force
                        }
                    $DataVariableQA += $_

                    # Increment the $i counter variable which is used to create the progress bar.
                    $i = $i+1

                    # Determine the completion percentage
                    $Completed = ($i/$DataCount) * 100
                    Write-Progress -Activity "Aligning source object structure with schema" -Status "Progress:" -PercentComplete $Completed
            } -End {
                
                # return data from temporary array to original $Data
                $Data = $DataVariableQA
            }
        Return $Data
}


# Function Get-AzDataCollectionRuleNamingConventionSrv ($TableName)

Function Get-AzDataCollectionRuleNamingConventionSrv
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [string]$TableName
         )

    # variables to be used for upload of data using DCR/log ingest api
    $DcrName    = "dcr-" + $Global:AzDcrPrefixSrvNetworkCloud + "-" + $TableName + "_CL"
    $DceName    = $Global:AzDceNameSrvNetworkCloud

    Return $DcrName, $DceName
}

#Function Get-AzDataCollectionRuleNamingConventionClt ($TableName)
Function Get-AzDataCollectionRuleNamingConventionClt
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [string]$TableName
         )

    # variables to be used for upload of data using DCR/log ingest api
    $DcrName    = "dcr-" + $Global:AzDcrPrefixClient + "-" + $TableName + "_CL"
    $DceName    = $Global:AzDceNameClient

    Return $DcrName, $DceName
}

#Function Get-AzLogAnalyticsTableAzDataCollectionRuleStatus ($AzLogWorkspaceResourceId, $TableName, $DcrName, $SchemaSourceObject, $AzAppId, $AzAppSecret, $TenantId)

Function Get-AzLogAnalyticsTableAzDataCollectionRuleStatus
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [string]$AzLogWorkspaceResourceId,
            [Parameter(mandatory)]
                [string]$TableName,
            [Parameter(mandatory)]
                [string]$DcrName,
            [Parameter(mandatory)]
                [array]$SchemaSourceObject,
            [Parameter()]
                [string]$AzAppId,
            [Parameter()]
                [string]$AzAppSecret,
            [Parameter()]
                [string]$TenantId
         )


    Write-host "  Checking LogAnalytics table and Data Collection Rule configuration .... Please Wait !"

    # by default ($false)
    $AzDcrDceTableCustomLogCreateUpdate = $false     # $True/$False - typically used when updates to schema detected

    #--------------------------------------------------------------------------
    # Connection
    #--------------------------------------------------------------------------

        $Headers = Get-AzAccessTokenManagement -AzAppId $AzAppId `
                                               -AzAppSecret $AzAppSecret `
                                               -TenantId $TenantId

        #--------------------------------------------------------------------------
        # Check if Azure LogAnalytics Table exist
        #--------------------------------------------------------------------------

            $TableUrl = "https://management.azure.com" + $AzLogWorkspaceResourceId + "/tables/$($TableName)_CL?api-version=2021-12-01-preview"
            $TableStatus = Try
                                {
                                    Invoke-RestMethod -Uri $TableUrl -Method GET -Headers $Headers
                                }
                           Catch
                                {
                                    Write-host "  LogAnalytics table wasn't found !"
                                    # initial setup - force to auto-create structure
                                    $AzDcrDceTableCustomLogCreateUpdate = $true     # $True/$False - typically used when updates to schema detected
                                }

        #--------------------------------------------------------------------------
        # Compare schema between source object schema and Azure LogAnalytics Table
        #--------------------------------------------------------------------------

            If ($TableStatus)
                {
                    $CurrentTableSchema = $TableStatus.properties.schema.columns

                    # Checking number of objects in schema
                        $CurrentTableSchemaCount = $CurrentTableSchema.count
                        $SchemaSourceObjectCount = ($SchemaSourceObject.count) + 1  # add 1 because TimeGenerated will automatically be added

                        If ($SchemaSourceObjectCount -gt $CurrentTableSchemaCount)
                            {
                               Write-host "  Schema mismatch - Schema source object contains more properties than defined in current schema"
                               $AzDcrDceTableCustomLogCreateUpdate = $true     # $True/$False - typically used when updates to schema detected
                            }

                    # Verify LogAnalytics table schema matches source object ($SchemaSourceObject) - otherwise set flag to update schema in LA/DCR
<#
                        ForEach ($Entry in $SchemaSourceObject)
                            {
                                $ChkSchema = $CurrentTableSchema | Where-Object { ($_.name -eq $Entry.name) -and ($_.type -eq $Entry.type) }

                                If ($ChkSchema -eq $null)
                                    {
                                        Write-host "  Schema mismatch - property missing or different type (name: $($Entry.name), type: $($Entry.type))"
                                        # Set flag to update schema
                                        $AzDcrDceTableCustomLogCreateUpdate = $true     # $True/$False - typically used when updates to schema detected
                                    }
                            }
#>
                }

        #--------------------------------------------------------------------------
        # Check if Azure Data Collection Rule exist
        #--------------------------------------------------------------------------

            # Check in global variable
            $DcrInfo = $global:AzDcrDetails | Where-Object { $_.name -eq $DcrName }
                If (!($DcrInfo))
                    {
                        Write-host "  DCR was not found [ $($DcrName) ]"
                        # initial setup - force to auto-create structure
                        $AzDcrDceTableCustomLogCreateUpdate = $true     # $True/$False - typically used when updates to schema detected
                    }

            If ($AzDcrDceTableCustomLogCreateUpdate -eq $false)
                {
                    Write-host "  Success - Schema & DCR structure is OK"
                }

        Return $AzDcrDceTableCustomLogCreateUpdate
    }


#Function Add-ColumnDataToAllEntriesInArray ($Column1Name, $Column1Data, $Column2Name, $Column2Data, $Column3Name, $Column3Data, $Data)

Function Add-ColumnDataToAllEntriesInArray
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data,
            [Parameter(mandatory)]
                [string]$Column1Name,
            [Parameter(mandatory)]
                [string]$Column1Data,
            [Parameter()]
                [string]$Column2Name,
            [Parameter()]
                [string]$Column2Data,
            [Parameter()]
                [string]$Column3Name,
            [Parameter()]
                [string]$Column3Data
         )

    Write-host "  Adding columns to all entries in array .... please wait !"
    $IntermediateObj = @()
    ForEach ($Entry in $Data)
        {
            If ($Column1Name)
                {
                    $Entry | Add-Member -MemberType NoteProperty -Name $Column1Name -Value $Column1Data -Force
                }

            If ($Column2Name)
                {
                    $Entry | Add-Member -MemberType NoteProperty -Name $Column2Name -Value $Column2Data -Force
                }

            If ($Column3Name)
                {
                    $Entry | Add-Member -MemberType NoteProperty -Name $Column3Name -Value $Column3Data -Force
                }

            $IntermediateObj += $Entry
        }
    return $IntermediateObj
}

<#
Function Add-CollectionTimeToAllEntriesInArray
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data
         )
#>

Function Add-CollectionTimeToAllEntriesInArray
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data
         )

    [datetime]$CollectionTime = ( Get-date ([datetime]::Now.ToUniversalTime()) -format "yyyy-MM-ddTHH:mm:ssK" )

    Write-host "  Adding CollectionTime to all entries in array .... please wait !"
    $IntermediateObj = @()
    ForEach ($Entry in $Data)
        {
            $Entry | Add-Member -MemberType NoteProperty -Name CollectionTime -Value $CollectionTime -Force | Out-Null

            $IntermediateObj += $Entry
        }

    return $IntermediateObj

}

Function OKAdd-CollectionTimeToAllEntriesInArray ($Data)
{
    [datetime]$CollectionTime = ( Get-date ([datetime]::Now.ToUniversalTime()) -format "yyyy-MM-ddTHH:mm:ssK" )

    Write-host "  Adding CollectionTime to all entries in array .... please wait !"
    $IntermediateObj = @()
    ForEach ($Entry in $Data)
        {
            $Entry | Add-Member -MemberType NoteProperty -Name CollectionTime -Value $CollectionTime -Force

            $IntermediateObj += $Entry
        }
    return $IntermediateObj
}


# Function Convert-CimArrayToObjectFixStructure ($Data)

Function Convert-CimArrayToObjectFixStructure
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data
         )

    Write-host "  Converting CIM array to Object & removing CIM class data in array .... please wait !"

    # Convert from array to object
    $Object = $Data | ConvertTo-Json | ConvertFrom-Json 

    # remove CIM info columns from object
    $ObjectModified = $Object | Select-Object -Property * -ExcludeProperty CimClass, CimInstanceProperties, CimSystemProperties

    return $ObjectModified
}

#Function Convert-PSArrayToObjectFixStructure ($Data)

Function Convert-PSArrayToObjectFixStructure
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data
         )

    Write-host "  Converting PS array to Object & removing PS class data in array .... please wait !"

    # Convert from array to object
    $Object = $Data | ConvertTo-Json | ConvertFrom-Json 

    # remove CIM info columns from object
    $ObjectModified = $Object | Select-Object -Property * -ExcludeProperty PSPath, PSProvider, PSParentPath, PSDrive, PSChildName, PSSnapIn

    return $ObjectModified
}


# Function Get-ObjectSchema ($Data, $ReturnType, $ReturnFormat)

Function Get-ObjectSchemaAsArray
{

    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data,
            [Parameter()]
                [ValidateSet("Table", "DCR")]
                [string[]]$ReturnType
         )


        $SchemaArrayLogAnalyticsTableFormat = @()
        $SchemaArrayDcrFormat = @()
        $SchemaArrayLogAnalyticsTableFormatHash = @()
        $SchemaArrayDcrFormatHash = @()

        # Requirement - Add TimeGenerated to array
        $SchemaArrayLogAnalyticsTableFormatHash += @{
                                                     name        = "TimeGenerated"
                                                     type        = "datetime"
                                                     description = ""
                                                    }

        $SchemaArrayLogAnalyticsTableFormat += [PSCustomObject]@{
                                                     name        = "TimeGenerated"
                                                     type        = "datetime"
                                                     description = ""
                                               }

        # Loop source object and build hash for table schema
        ForEach ($Entry in $Data)
            {
                $ObjColumns = $Entry | ConvertTo-Json -Depth 100 | ConvertFrom-Json | Get-Member -MemberType NoteProperty
                ForEach ($Column in $ObjColumns)
                    {
                        $ObjDefinitionStr = $Column.Definition
                                If ($ObjDefinitionStr -like "int*")                                            { $ObjType = "int" }
                            ElseIf ($ObjDefinitionStr -like "real*")                                           { $ObjType = "int" }
                            ElseIf ($ObjDefinitionStr -like "long*")                                           { $ObjType = "long" }
                            ElseIf ($ObjDefinitionStr -like "guid*")                                           { $ObjType = "dynamic" }
                            ElseIf ($ObjDefinitionStr -like "string*")                                         { $ObjType = "string" }
                            ElseIf ($ObjDefinitionStr -like "datetime*")                                       { $ObjType = "datetime" }
                            ElseIf ($ObjDefinitionStr -like "bool*")                                           { $ObjType = "boolean" }
                            ElseIf ($ObjDefinitionStr -like "object*")                                         { $ObjType = "dynamic" }
                            ElseIf ($ObjDefinitionStr -like "System.Management.Automation.PSCustomObject*")    { $ObjType = "dynamic" }

                        # build for array check
                        $SchemaLogAnalyticsTableFormatObjHash = @{
                                                                   name        = $Column.Name
                                                                   type        = $ObjType
                                                                   description = ""
                                                                 }

                        $SchemaLogAnalyticsTableFormatObj     = [PSCustomObject]@{
                                                                   name        = $Column.Name
                                                                   type        = $ObjType
                                                                   description = ""
                                                                }
                        $SchemaDcrFormatObjHash = @{
                                                      name        = $Column.Name
                                                      type        = $ObjType
                                                   }

                        $SchemaDcrFormatObj     = [PSCustomObject]@{
                                                      name        = $Column.Name
                                                      type        = $ObjType
                                                  }


                        If ($Column.Name -notin $SchemaArrayLogAnalyticsTableFormat.name)
                            {
                                $SchemaArrayLogAnalyticsTableFormat       += $SchemaLogAnalyticsTableFormatObj
                                $SchemaArrayDcrFormat                     += $SchemaDcrFormatObj

                                $SchemaArrayLogAnalyticsTableFormatHash   += $SchemaLogAnalyticsTableFormatObjHash
                                $SchemaArrayDcrFormatHash                 += $SchemaDcrFormatObjHash
                            }
                    }
            }

            If ($ReturnType -eq "Table")
            {
                # Return schema format for LogAnalytics table
                Return $SchemaArrayLogAnalyticsTableFormat
            }
        ElseIf ($ReturnType -eq "DCR")
            {
                # Return schema format for DCR
                Return $SchemaArrayDcrFormat
            }
        Else
            {
                # Return schema format for DCR
                Return $SchemaArrayDcrFormat
            }

}

Function Get-ObjectSchemaAsHash
{

    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data,
            [Parameter(mandatory)]
                [ValidateSet("Table", "DCR")]
                [string[]]$ReturnType
         )


        $SchemaArrayLogAnalyticsTableFormat = @()
        $SchemaArrayDcrFormat = @()
        $SchemaArrayLogAnalyticsTableFormatHash = @()
        $SchemaArrayDcrFormatHash = @()

        # Requirement - Add TimeGenerated to array
        $SchemaArrayLogAnalyticsTableFormatHash += @{
                                                     name        = "TimeGenerated"
                                                     type        = "datetime"
                                                     description = ""
                                                    }

        $SchemaArrayLogAnalyticsTableFormat += [PSCustomObject]@{
                                                     name        = "TimeGenerated"
                                                     type        = "datetime"
                                                     description = ""
                                               }

        # Loop source object and build hash for table schema
        ForEach ($Entry in $Data)
            {
                $ObjColumns = $Entry | ConvertTo-Json -Depth 100 | ConvertFrom-Json | Get-Member -MemberType NoteProperty
                ForEach ($Column in $ObjColumns)
                    {
                        $ObjDefinitionStr = $Column.Definition
                                If ($ObjDefinitionStr -like "int*")                                            { $ObjType = "int" }
                            ElseIf ($ObjDefinitionStr -like "real*")                                           { $ObjType = "int" }
                            ElseIf ($ObjDefinitionStr -like "long*")                                           { $ObjType = "long" }
                            ElseIf ($ObjDefinitionStr -like "guid*")                                           { $ObjType = "dynamic" }
                            ElseIf ($ObjDefinitionStr -like "string*")                                         { $ObjType = "string" }
                            ElseIf ($ObjDefinitionStr -like "datetime*")                                       { $ObjType = "datetime" }
                            ElseIf ($ObjDefinitionStr -like "bool*")                                           { $ObjType = "boolean" }
                            ElseIf ($ObjDefinitionStr -like "object*")                                         { $ObjType = "dynamic" }
                            ElseIf ($ObjDefinitionStr -like "System.Management.Automation.PSCustomObject*")    { $ObjType = "dynamic" }

                        # build for array check
                        $SchemaLogAnalyticsTableFormatObjHash = @{
                                                                   name        = $Column.Name
                                                                   type        = $ObjType
                                                                   description = ""
                                                                 }

                        $SchemaLogAnalyticsTableFormatObj     = [PSCustomObject]@{
                                                                   name        = $Column.Name
                                                                   type        = $ObjType
                                                                   description = ""
                                                                }
                        $SchemaDcrFormatObjHash = @{
                                                      name        = $Column.Name
                                                      type        = $ObjType
                                                   }

                        $SchemaDcrFormatObj     = [PSCustomObject]@{
                                                      name        = $Column.Name
                                                      type        = $ObjType
                                                  }


                        If ($Column.Name -notin $SchemaArrayLogAnalyticsTableFormat.name)
                            {
                                $SchemaArrayLogAnalyticsTableFormat       += $SchemaLogAnalyticsTableFormatObj
                                $SchemaArrayDcrFormat                     += $SchemaDcrFormatObj

                                $SchemaArrayLogAnalyticsTableFormatHash   += $SchemaLogAnalyticsTableFormatObjHash
                                $SchemaArrayDcrFormatHash                 += $SchemaDcrFormatObjHash
                            }
                    }
            }

            If ($ReturnType -eq "Table")
            {
                # Return schema format for Table
                $SchemaArrayLogAnalyticsTableFormatHash
            }
        ElseIf ($ReturnType -eq "DCR")
            {
                # Return schema format for DCR
                $SchemaArrayDcrFormatHash
            }
        
        Return

}

# Function Filter-ObjectExcludeProperty ($Data, $ExcludeProperty)

Function Filter-ObjectExcludeProperty
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data,
            [Parameter(mandatory)]
                [array]$ExcludeProperty
         )

    $Data = $Data | Select-Object * -ExcludeProperty $ExcludeProperty
    Return $Data
}

# Function Get-AzDcrListAll ($AzAppId, $AzAppSecret, $TenantId)

Function Get-AzDcrListAll
{

    [CmdletBinding()]
    param(
            [Parameter()]
                [string]$AzAppId,
            [Parameter()]
                [string]$AzAppSecret,
            [Parameter()]
                [string]$TenantId
         )

    Write-host ""
    Write-host "Getting Data Collection Rules from Azure Resource Graph .... Please Wait !"

    #--------------------------------------------------------------------------
    # Connection
    #--------------------------------------------------------------------------

        $Headers = Get-AzAccessTokenManagement -AzAppId $AzAppId `
                                               -AzAppSecret $AzAppSecret `
                                               -TenantId $TenantId

    #--------------------------------------------------------------------------
    # Get DCRs from Azure Resource Graph
    #--------------------------------------------------------------------------

        $AzGraphQuery = @{
                            'query' = 'Resources | where type =~ "microsoft.insights/datacollectionrules" '
                            } | ConvertTo-Json -Depth 20

        $ResponseData = @()

        $AzGraphUri          = "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2021-03-01"
        $ResponseRaw         = Invoke-WebRequest -Method POST -Uri $AzGraphUri -Headers $Headers -Body $AzGraphQuery
        $ResponseData       += $ResponseRaw.content
        $ResponseNextLink    = $ResponseRaw."@odata.nextLink"

        While ($ResponseNextLink -ne $null)
            {
                $ResponseRaw         = Invoke-WebRequest -Method POST -Uri $AzGraphUri -Headers $Headers -Body $AzGraphQuery
                $ResponseData       += $ResponseRaw.content
                $ResponseNextLink    = $ResponseRaw."@odata.nextLink"
            }
        $DataJson = $ResponseData | ConvertFrom-Json
        $Data     = $DataJson.data

        Return $Data
}

#Function Get-AzDceListAll ($AzAppId, $AzAppSecret, $TenantId)

Function Get-AzDceListAll
{

    [CmdletBinding()]
    param(
            [Parameter()]
                [string]$AzAppId,
            [Parameter()]
                [string]$AzAppSecret,
            [Parameter()]
                [string]$TenantId
         )

    Write-host ""
    Write-host "Getting Data Collection Endpoints from Azure Resource Graph .... Please Wait !"

    #--------------------------------------------------------------------------
    # Connection
    #--------------------------------------------------------------------------

        $Headers = Get-AzAccessTokenManagement -AzAppId $AzAppId `
                                               -AzAppSecret $AzAppSecret `
                                               -TenantId $TenantId

    #--------------------------------------------------------------------------
    # Get DCEs from Azure Resource Graph
    #--------------------------------------------------------------------------

        $AzGraphQuery = @{
                            'query' = 'Resources | where type =~ "microsoft.insights/datacollectionendpoints" '
                            } | ConvertTo-Json -Depth 20

        $ResponseData = @()

        $AzGraphUri          = "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2021-03-01"
        $ResponseRaw         = Invoke-WebRequest -Method POST -Uri $AzGraphUri -Headers $Headers -Body $AzGraphQuery
        $ResponseData       += $ResponseRaw.content
        $ResponseNextLink    = $ResponseRaw."@odata.nextLink"

        While ($ResponseNextLink -ne $null)
            {
                $ResponseRaw         = Invoke-WebRequest -Method POST -Uri $AzGraphUri -Headers $Headers -Body $AzGraphQuery
                $ResponseData       += $ResponseRaw.content
                $ResponseNextLink    = $ResponseRaw."@odata.nextLink"
            }
        $DataJson = $ResponseData | ConvertFrom-Json
        $Data     = $DataJson.data

        Return $Data
}

#Function Post-AzLogAnalyticsLogIngestCustomLogDcrDce-Output ($Data, $DcrName, $DceName, $AzAppId, $AzAppSecret, $TenantId, $BatchAmount)
Function Post-AzLogAnalyticsLogIngestCustomLogDcrDce-Output
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data,
            [Parameter(mandatory)]
                [string]$DcrName,
            [Parameter(mandatory)]
                [string]$DceName,
            [Parameter()]
                [string]$BatchAmount,
            [Parameter()]
                [string]$AzAppId,
            [Parameter()]
                [string]$AzAppSecret,
            [Parameter()]
                [string]$TenantId
         )


        $AzDcrDceDetails = Get-AzDcrDceDetails -DcrName $DcrName -DceName $DceName `
                                               -AzAppId $LogIngestAppId -AzAppSecret $LogIngestAppSecret -TenantId $TenantId

        Post-AzLogAnalyticsLogIngestCustomLogDcrDce  -DceUri $AzDcrDceDetails[2] -DcrImmutableId $AzDcrDceDetails[6] `
                                                     -DcrStream $AzDcrDceDetails[7] -Data $DataVariable -BatchAmount $BatchAmount `
                                                     -AzAppId $LogIngestAppId -AzAppSecret $LogIngestAppSecret -TenantId $TenantId
        
        # Write result to screen
        $DataVariable | Out-String | Write-Verbose 
}

<#
Function CheckCreateUpdate-TableDcr-Structure ($Data, $AzLogWorkspaceResourceId, $TableName, $DcrName, $DceName, $SchemaSourceObject, `
                                               $AzAppId, $AzAppSecret, $TenantId, $LogIngestServicePricipleObjectId, $AzDcrSetLogIngestApiAppPermissionsDcrLevel)
#>

Function CheckCreateUpdate-TableDcr-Structure
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data,
            [Parameter(mandatory)]
                [string]$AzLogWorkspaceResourceId,
            [Parameter(mandatory)]
                [string]$TableName,
            [Parameter(mandatory)]
                [string]$DcrName,
            [Parameter(mandatory)]
                [string]$DceName,
            [Parameter(mandatory)]
                [string]$LogIngestServicePricipleObjectId,
            [Parameter(mandatory)]
                [string]$AzDcrSetLogIngestApiAppPermissionsDcrLevel,
            [Parameter(mandatory)]
                [boolean]$AzLogDcrTableCreateFromAnyMachine,
            [Parameter(mandatory)]
                [AllowEmptyCollection()]
                [array]$AzLogDcrTableCreateFromReferenceMachine,
            [Parameter()]
                [string]$AzAppId,
            [Parameter()]
                [string]$AzAppSecret,
            [Parameter()]
                [string]$TenantId
         )

    #-------------------------------------------------------------------------------------------
    # Create/Update Schema for LogAnalytics Table & Data Collection Rule schema
    #-------------------------------------------------------------------------------------------

        If ( ($AzAppId) -and ($AzAppSecret) )
            {
                #-----------------------------------------------------------------------------------------------
                # Check if table and DCR exist - or schema must be updated due to source object schema changes
                #-----------------------------------------------------------------------------------------------
                    
                    # Get insight about the schema structure
                    $Schema = Get-ObjectSchemaAsArray -Data $Data
                    $StructureCheck = Get-AzLogAnalyticsTableAzDataCollectionRuleStatus -AzLogWorkspaceResourceId $AzLogWorkspaceResourceId -TableName $TableName -DcrName $DcrName -SchemaSourceObject $Schema `
                                                                                        -AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId

                #-----------------------------------------------------------------------------------------------
                # Structure check = $true -> Create/update table & DCR with necessary schema
                #-----------------------------------------------------------------------------------------------

                    If ($StructureCheck -eq $true)
                        {
                            If ( ( $env:COMPUTERNAME -in $AzLogDcrTableCreateFromReferenceMachine) -or ($AzLogDcrTableCreateFromAnyMachine -eq $true) )    # manage table creations
                                {
                                    # build schema to be used for LogAnalytics Table
                                    $Schema = Get-ObjectSchemaAsHash -Data $Data -ReturnType Table

                                    CreateUpdate-AzLogAnalyticsCustomLogTableDcr -AzLogWorkspaceResourceId $AzLogWorkspaceResourceId -SchemaSourceObject $Schema -TableName $TableName `
                                                                                 -AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId 


                                    # build schema to be used for DCR
                                    $Schema = Get-ObjectSchemaAsHash -Data $DataVariable -ReturnType DCR

                                    CreateUpdate-AzDataCollectionRuleLogIngestCustomLog -AzLogWorkspaceResourceId $AzLogWorkspaceResourceId -SchemaSourceObject $Schema `
                                                                                        -DceName $DceName -DcrName $DcrName -TableName $TableName `
                                                                                        -LogIngestServicePricipleObjectId $LogIngestServicePricipleObjectId `
                                                                                        -AzDcrSetLogIngestApiAppPermissionsDcrLevel $AzDcrSetLogIngestApiAppPermissionsDcrLevel `
                                                                                        -AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId
                                }
                        }
                } # create table/DCR
}

