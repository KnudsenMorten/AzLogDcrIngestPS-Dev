Function Update-AzDataCollectionRuleLogAnalyticsCustomLogTableSchema
{
 <#
    .SYNOPSIS
    Updates the schema of Azure Loganalytics table + Azure Data Collection Rule - based on source object schema

    Developed by Morten Knudsen, Microsoft MVP

    .DESCRIPTION
    Used to ensure DCR and LogAnalytics table can accept the structure/schema coming from the source object

    .PARAMETER SchemaSourceObject
    This is the schema in hash table format coming from the source object

    .PARAMETER Tablename
    Specifies the table name in LogAnalytics

    .PARAMETER DcrResourceId
    This is resource id of the Data Collection Rule

    .PARAMETER AzLogWorkspaceResourceId
    This is the Loganaytics Resource Id

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
    PS> Update-AzDataCollectionRuleLogAnalyticsCustomLogTableSchema

 #>

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
                                               -TenantId $TenantId -Verbose:$Verbose

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

