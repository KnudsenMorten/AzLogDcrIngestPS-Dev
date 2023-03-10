Function Delete-AzLogAnalyticsCustomLogTables
{
 <#
    .SYNOPSIS
    Deletes the Azure Loganalytics defined in like-format, so you can fast clean-up for example after demo or testing

    .DESCRIPTION
    Used to delete many tables in one task

    .PARAMETER TableNameLike
    Here you can put in the table name(s) you wan to delete using like-format - sample *demo* 

    .PARAMETER AzLogWorkspaceResourceId
    This is resource id of the Azure LogAnalytics workspace

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
    PS> Delete-AzLogAnalyticsCustomLogTables -TableLike *demo* will delete all tables with the word demo in it
 #>

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
                                               -TenantId $TenantId -Verbose:$Verbose


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

