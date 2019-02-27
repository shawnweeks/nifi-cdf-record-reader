cdfVarSchema = SchemaBuilder
        .record("cdfVar")
        .fields()
        .name("file_id").type().stringType().noDefault()
        .name("variable_name").type().stringType().noDefault()
        .name("variable_type").type().stringType().noDefault()
        .name("num_elements").type().intType().noDefault()
        .name("dim").type().intType().noDefault()
        .name("dim_sizes").type().array().items().intType().noDefault()
        .name("dim_variances").type().array().items().booleanType().noDefault()
        .name("rec_variance").type().intType().noDefault()
        .name("max_records").type().intType().noDefault()
        .name("attributes").type().map().values(cdfVarAttSchema).noDefault()
        .endRecord();