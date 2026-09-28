[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"bronze order_data_typed" = let
  // Connection: Fabric Lakehouse -> workspace -> lakehouse -> table
  Source = Lakehouse.Contents([HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  #"Navigation 1" = Source{[workspaceId = "8ae2f35a-88b5-4348-897d-09d731cd5007"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "0241cb38-6edc-4bdf-87f1-433be207885f"]}[Data],
  // Source table: bronze.order_data_typed
  SourceTable = #"Navigation 2"{[Id = "bronze.order_data_typed", ItemKind = "Table"]}[Data],

  // Strip formatting characters and upper-case; empty -> null
  CleanKey = (value as any) as nullable text =>
        let
            raw_text = Text.Trim(Text.From(value)),
            unformatted = Text.Select(raw_text, {"A".."Z", "a".."z", "0".."9"}),
            result = if unformatted = "" then null else Text.Upper(unformatted)
        in
            result,

  // Apply the function to the four business keys
  #"Standardized keys" = Table.TransformColumns(
        SourceTable,
        {
            {"SERVICE_ORDER_NUMBER",   each CleanKey(_), type nullable text},
            {"RESTAURANT_LOCATION_ID", each CleanKey(_), type nullable text},
            {"PRODUCT_NUMBER",         each CleanKey(_), type nullable text},
            {"CUSTOMER_NUMBER",        each CleanKey(_), type nullable text}
        }
    ),

  // Flag rows that would be quarantined in silver (missing restaurant id)
  #"Quality flag" = Table.AddColumn(
        #"Standardized keys",
        "is_valid_for_silver",
        each [RESTAURANT_LOCATION_ID] <> null,
        type logical
    )
in
  #"Quality flag";
