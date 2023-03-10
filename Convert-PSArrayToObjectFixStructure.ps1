Function Convert-PSArrayToObjectFixStructure
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data
         )

    Write-Verbose "  Converting PS array to Object & removing PS class data in array .... please wait !"

    # Convert from array to object
    $Object = $Data | ConvertTo-Json -Depth 20 | ConvertFrom-Json 

    # remove CIM info columns from object
    $ObjectModified = $Object | Select-Object -Property * -ExcludeProperty PSPath, PSProvider, PSParentPath, PSDrive, PSChildName, PSSnapIn

    return $ObjectModified
}

