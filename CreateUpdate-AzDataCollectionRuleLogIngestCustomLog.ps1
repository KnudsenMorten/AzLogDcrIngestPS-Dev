Function CreateUpdate-AzDataCollectionRuleLogIngestCustomLog
{
 <#
    .SYNOPSIS
    Create or Update Azure Data Collection Rule (DCR) used for log ingestion to Azure LogAnalytics using
    Log Ingestion API

    .DESCRIPTION
    Uses schema based on source object

    .PARAMETER Tablename
    Specifies the table name in LogAnalytics

    .PARAMETER SchemaSourceObject
    This is the schema in hash table format coming from the source object

    .PARAMETER AzLogWorkspaceResourceId
    This is the Loganaytics Resource Id

    .PARAMETER DceName
    This is name of the Data Collection Endpoint to use for the upload
    Function will automatically look check in a global variable ($global:AzDceDetails) - or do a query using Azure Resource Graph to find DCE with name
    Goal is to find the log ingestion Uri on the DCE

    Variable $global:AzDceDetails can be build before calling this cmdlet using this syntax
    $global:AzDceDetails = Get-AzDceListAll -AzAppId $LogIngestAppId -AzAppSecret $LogIngestAppSecret -TenantId $TenantId -Verbose:$Verbose -Verbose:$Verbose
 
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
    This is the Azure app id og an app with Contributor permissions in LogAnalytics + Resource Group for DCRs
        
    .PARAMETER AzAppSecret
    This is the secret of the Azure app

    .PARAMETER TenantId
    This is the Azure AD tenant id

    .INPUTS
    None. You cannot pipe objects

    .OUTPUTS
    Output of REST PUT command. Should be 200 for success

    .EXAMPLE
    PS> CreateUpdate-AzDataCollectionRuleLogIngestCustomLog -AzLogWorkspaceResourceId $AzLogWorkspaceResourceId -SchemaSourceObject $Schema `
															-DceName $DceName -DcrName $DcrName -TableName $TableName `
															-LogIngestServicePricipleObjectId $LogIngestServicePricipleObjectId `
															-AzDcrSetLogIngestApiAppPermissionsDcrLevel $AzDcrSetLogIngestApiAppPermissionsDcrLevel `
															-AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId -Verbose:$Verbose


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
                [string]$DcrName,
            [Parameter(mandatory)]
                [string]$TableName,
            [Parameter(mandatory)]
                [boolean]$AzDcrSetLogIngestApiAppPermissionsDcrLevel,
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
                         } | ConvertTo-Json -Depth 10

                Write-Verbose "Creating Resource group $($DcrResourceGroup) ... Please Wait !"
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

        Write-Verbose ""
        Write-Verbose "Creating/updating DCR [ $($DcrName) ] with limited payload"
        Write-Verbose $DcrResourceId

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

        Write-Verbose ""
        Write-Verbose "Updating DCR [ $($DcrName) ] with full schema"
        Write-Verbose $DcrResourceId

        $DcrPayload = $DcrObject | ConvertTo-Json -Depth 20

        $Uri = "https://management.azure.com" + "$DcrResourceId" + "?api-version=2022-06-01"
        Invoke-WebRequest -Uri $Uri -Method PUT -Body $DcrPayload -Headers $Headers

    #--------------------------------------------------------------------------
    # sleep 10 sec to let Azure Resource Graph pick up the new DCR
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
