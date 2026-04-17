/// <reference path="./.sst/platform/config.d.ts" />

export default $config({
    app(input) {
        return {
            name: "dcb-event-store-bench",
            removal: input?.stage === "production" ? "retain" : "remove",
            home: "aws",
            providers: {
                aws: { region: process.env.AWS_REGION ?? "eu-west-1" },
            },
        }
    },
    async run() {
        const vpc = new sst.aws.Vpc("BenchVpc", {
            az: 2,
            nat: "ec2",
        })

        const aurora = new sst.aws.Aurora("EventStoreDb", {
            engine: "postgres",
            version: "17.4",
            vpc,
            scaling: {
                min: process.env.AURORA_MIN_ACU ?? "32 ACU",
                max: process.env.AURORA_MAX_ACU ?? "64 ACU",
            },
            proxy: true,
        })

        const pgConnectionString = $interpolate`postgresql://${aurora.username}:${aurora.password}@${aurora.host}:${aurora.port}/${aurora.database}`
        const lockMode = process.env.LOCK_MODE ?? "row"

        const benchSuite = new sst.aws.Function("PgStressSuite", {
            handler: "src/lambda-aurora.handler",
            runtime: "nodejs20.x",
            architecture: "arm64",
            timeout: "15 minutes",
            memory: "4096 MB",
            vpc,
            environment: {
                PG_CONNECTION_STRING: pgConnectionString,
                LOCK_MODE: lockMode,
            },
        })

        return {
            vpcId: vpc.id,
            auroraHost: aurora.host,
            auroraDatabase: aurora.database,
            benchSuiteName: benchSuite.name,
        }
    },
})
