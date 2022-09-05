// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package sample;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.UUID;
import java.net.URI;
import java.time.OffsetDateTime;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.core.http.policy.HttpLogOptions;
import com.azure.core.management.exception.ManagementException;
import com.azure.core.management.profile.AzureProfile;
import com.azure.resourcemanager.mediaservices.models.*;
import com.azure.resourcemanager.mediaservices.MediaServicesManager;
import com.azure.identity.ClientSecretCredentialBuilder;

import javax.naming.AuthenticationException;

public class EncodingWithMESPredefinedPreset {
    private static final String TRANSFORM_NAME = "AdaptiveBitrate";
    private static final String OUTPUT_FOLDER = "Output";
    private static final String BASE_URI = "https://nimbuscdn-nimbuspm.streaming.mediaservices.windows.net/2b533311-b215-4409-80af-529c3e853622/";
    private static final String MP4_FILE_NAME = "Ignite-short.mp4";
    private static final String INPUT_LABEL = "input1";

    // Por favor, altere isso para o nome do seu endpoint
    private static final String STREAMING_ENDPOINT_NAME = "default";

    public static void main(String[] args) {

        // Certifique-se de ter definido a configuração em resources/conf/appsettings.json. Para mais informações, veja
        // https://docs.microsoft.com/azure/media-services/latest/access-api-cli-how-to.

        ConfigWrapper config = new ConfigWrapper();
        runEncodingWithMESPredefinedPreset(config);

        config.close();
        System.exit(0);
    }

    /**
     * Run the sample.
     *
     * @param config Este parâmetro é do tipo ConfigWrapper. Esta classe lê valores do arquivo de configuração local.
     */
    private static void runEncodingWithMESPredefinedPreset(ConfigWrapper config) {

        // Conecte-se a serviços de mídia, consulte https://docs.microsoft.com/en-us/azure/media-services/latest/configure-connect-java-howto
        // para detalhes.

        TokenCredential credential = new ClientSecretCredentialBuilder()
                .clientId(config.getAadClientId())
                .clientSecret(config.getAadSecret())
                .tenantId(config.getAadTenantId())
                .build();
        AzureProfile profile = new AzureProfile(config.getAadTenantId(), config.getSubscriptionId(),
                com.azure.core.management.AzureEnvironment.AZURE);


        // MediaServiceManager é o ponto de entrada para o gerenciamento de recursos do Azure Media.

        MediaServicesManager manager = MediaServicesManager.configure()
                .withLogOptions(new HttpLogOptions().setLogLevel(HttpLogDetailLevel.BODY_AND_HEADERS))
                .authenticate(credential, profile);


        // Criando um sufixo exclusivo para que não tenhamos colisões de nomes se você executar o
        // amostra

        UUID uuid = UUID.randomUUID();
        String uniqueness = uuid.toString();
        String jobName = "job-" + uniqueness.substring(0, 13);
        String locatorName = "locator-" + uniqueness;
        String outputAssetName = "output-" + uniqueness;
        boolean stopEndpoint = false;

        Scanner scanner = new Scanner(System.in);
        try {
            List<TransformOutput> outputs = new ArrayList<>();
            outputs.add(new TransformOutput().withPreset(
                    new BuiltInStandardEncoderPreset().withPresetName(EncoderNamedPreset.CONTENT_AWARE_ENCODING)));


        // Cria a transformação.

            System.out.println("Creating a transform...");
            Transform transform = manager.transforms()
                    .define(TRANSFORM_NAME)
                    .withExistingMediaService(config.getResourceGroup(), config.getAccountName())
                    .withOutputs(outputs)
                    .create();
            System.out.println("Transform created");


        // Cria um JobInputHttp. A entrada para o trabalho é um URL HTTPS apontando para um arquivo MP4.

            List<String> files = new ArrayList<>();
            files.add(MP4_FILE_NAME);
            JobInputHttp input = new JobInputHttp().withBaseUri(BASE_URI);
            input.withFiles(files);
            input.withLabel(INPUT_LABEL);


            // A saída do Job de codificação deve ser gravada em um Asset, então vamos criar um. Observe que nós
            // estão usando um nome de ativo exclusivo, não deve haver uma colisão de nomes.

            System.out.println("Creating an output asset...");
            Asset outputAsset = manager.assets()
                    .define(outputAssetName)
                    .withExistingMediaService(config.getResourceGroup(), config.getAccountName())
                    .create();

            Job job = submitJob(manager, config.getResourceGroup(), config.getAccountName(), transform.name(), jobName,
                    input, outputAsset.name());

            long startedTime = System.currentTimeMillis();

            // Neste código de demonstração, pesquisaremos o status do trabalho. A pesquisa não é uma prática recomendada para produção
            // aplicativos devido à latência que ele introduz. O uso excessivo dessa API pode acionar a limitação. Desenvolvedores
            // deve usar Event Grid. Para ver como implementar a grade de eventos, veja o exemplo
            // https://github.com/Azure-Samples/media-services-v3-java/tree/master/ContentProtection/BasicAESClearKey.

            job = waitForJobToFinish(manager, config.getResourceGroup(), config.getAccountName(), transform.name(),
                    jobName);

            long elapsed = (System.currentTimeMillis() - startedTime) / 1000; // Elapsed time in seconds
            System.out.println("Job elapsed time: " + elapsed + " second(s).");

            if (job.state() == JobState.FINISHED) {
                System.out.println("Job finished.");
                System.out.println();

                // Agora que o conteúdo foi codificado, publique-o para Streaming criando
                // um StreamingLocator.

                StreamingLocator locator = getStreamingLocator(manager, config.getResourceGroup(), config.getAccountName(),
                        outputAsset.name(), locatorName);

                StreamingEndpoint streamingEndpoint = manager.streamingEndpoints()
                        .get(config.getResourceGroup(), config.getAccountName(), STREAMING_ENDPOINT_NAME);

                if (streamingEndpoint != null) {

                // Inicia o Streaming Endpoint se não estiver em execução.

                    if (streamingEndpoint.resourceState() != StreamingEndpointResourceState.RUNNING) {
                        System.out.println("Streaming endpoint was stopped, restarting it...");
                        manager.streamingEndpoints().start(config.getResourceGroup(), config.getAccountName(), STREAMING_ENDPOINT_NAME);


                // Iniciamos o endpoint, devemos pará-lo na limpeza.

                        stopEndpoint = true;
                    }

                    System.out.println();
                    System.out.println("Streaming urls:");
                    List<String> urls = getStreamingUrls(manager, config.getResourceGroup(), config.getAccountName(), locator.name(), streamingEndpoint);

                    for (String url : urls) {
                        System.out.println("\t" + url);
                    }

                    System.out.println();
                    System.out.println("To stream, copy and paste the Streaming URL into the Azure Media Player at 'http://aka.ms/azuremediaplayer'.");
                    System.out.println("When finished, press ENTER to continue.");
                    System.out.println();
                    System.out.flush();
                    scanner.nextLine();


                // Baixe o ativo de saída para verificação.

                    System.out.println("Downloading output asset...");
                    System.out.println();
                    File outputFolder = new File(OUTPUT_FOLDER);
                    if (outputFolder.exists() && !outputFolder.isDirectory()) {
                        outputFolder = new File(OUTPUT_FOLDER + uniqueness);
                    }
                    if (!outputFolder.exists()) {
                        outputFolder.mkdir();
                    }

                    downloadResults(manager, config.getResourceGroup(), config.getAccountName(), outputAsset.name(),
                            outputFolder);

                    System.out.println("Done downloading. Please check the files at " + outputFolder.getAbsolutePath());
                } else {
                    System.out.println("Could not find streaming endpoint: " + STREAMING_ENDPOINT_NAME);
                }

                System.out.println("When finished, press ENTER to cleanup.");
                System.out.println();
                System.out.flush();
                scanner.nextLine();
            } else if (job.state() == JobState.ERROR) {
                System.out.println("ERROR: Job finished with error message: " + job.outputs().get(0).error().message());
                System.out.println("ERROR:                   error details: "
                        + job.outputs().get(0).error().details().get(0).message());
            }
        } catch (Exception e) {
            Throwable cause = e;
            while (cause != null) {
                if (cause instanceof AuthenticationException) {
                    System.out.println("ERROR: Authentication error, please check your account settings in appsettings.json.");
                    break;
                } else if (cause instanceof ManagementException) {
                    ManagementException apiException = (ManagementException) cause;
                    System.out.println("ERROR: " + apiException.getValue().getMessage());
                    break;
                }
                cause = cause.getCause();
            }
            System.out.println();
            e.printStackTrace();
            System.out.println();
        } finally {
            System.out.println("Cleaning up...");
            if (scanner != null) {
                scanner.close();
            }
            cleanup(manager, config.getResourceGroup(), config.getAccountName(), TRANSFORM_NAME, jobName,
                    outputAssetName, locatorName, stopEndpoint, STREAMING_ENDPOINT_NAME);
            System.out.println("Done.");
        }
    }

    /**
     * Create and submit a job.
     *
     * @param manager         The entry point of Azure Media resource management.
     * @param resourceGroup   The name of the resource group within the Azure subscription.
     * @param accountName     The Media Services account name.
     * @param transformName   The name of the transform.
     * @param jobName         The name of the job.
     * @param jobInput        The input to the job.
     * @param outputAssetName The name of the asset that the job writes to.
     * @return The job created.
     */
    private static Job submitJob(MediaServicesManager manager, String resourceGroup, String accountName, String transformName,
                                 String jobName, JobInput jobInput, String outputAssetName) {
        System.out.println("Creating a job...");
        // Primeiro especifique onde a(s) saída(s) do Job precisam ser gravadas
        List<JobOutput> jobOutputs = new ArrayList<>();
        jobOutputs.add(new JobOutputAsset().withAssetName(outputAssetName));

        Job job = manager.jobs().define(jobName)
                .withExistingTransform(resourceGroup, accountName, transformName)
                .withInput(jobInput)
                .withOutputs(jobOutputs)
                .create();

        return job;
    }

    /**
     * Polls Media Services for the status of the Job.
     *
     * @param manager       This is the entry point of Azure Media resource
     *                      management
     * @param resourceGroup The name of the resource group within the Azure
     *                      subscription
     * @param accountName   The Media Services account name
     * @param transformName The name of the transform
     * @param jobName       The name of the job submitted
     * @return The job
     */
    private static Job waitForJobToFinish(MediaServicesManager manager, String resourceGroup, String accountName,
                                          String transformName, String jobName) {
        final int SLEEP_INTERVAL = 10 * 1000;

        Job job = null;
        boolean exit = false;

        do {
            job = manager.jobs().get(resourceGroup, accountName, transformName, jobName);

            if (job.state() == JobState.FINISHED || job.state() == JobState.ERROR || job.state() == JobState.CANCELED) {
                exit = true;
            } else {
                System.out.println("Job is " + job.state());

                int i = 0;
                for (JobOutput output : job.outputs()) {
                    System.out.print("\tJobOutput[" + i++ + "] is " + output.state() + ".");
                    if (output.state() == JobState.PROCESSING) {
                        System.out.print("  Progress: " + output.progress());
                    }
                    System.out.println();
                }

                try {
                    Thread.sleep(SLEEP_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } while (!exit);

        return job;
    }

    /**
     * Use Media Service and Storage APIs to download the output files to a local folder
     *
     * @param manager       The entry point of Azure Media resource management
     * @param resourceGroup The name of the resource group within the Azure subscription
     * @param accountName   The Media Services account name
     * @param assetName     The asset name
     * @param outputFolder  The output folder for downloaded files.
     * @throws Exception
     * @throws URISyntaxException
     * @throws IOException
     */
    private static void downloadResults(MediaServicesManager manager, String resourceGroup, String accountName,
                                        String assetName, File outputFolder) throws URISyntaxException, IOException {
        ListContainerSasInput parameters = new ListContainerSasInput()
                .withPermissions(AssetContainerPermission.READ)
                .withExpiryTime(OffsetDateTime.now().plusHours(1));
        AssetContainerSas assetContainerSas = manager.assets()
                .listContainerSas(resourceGroup, accountName, assetName, parameters);

        BlobContainerClient container =
                new BlobContainerClientBuilder()
                        .endpoint(assetContainerSas.assetContainerSasUrls().get(0))
                        .buildClient();

        File directory = new File(outputFolder, assetName);
        directory.mkdir();

        container.listBlobs().forEach(blobItem -> {
            BlobClient blob = container.getBlobClient(blobItem.getName());
            File downloadTo = new File(directory, blobItem.getName());
            blob.downloadToFile(downloadTo.getAbsolutePath());
        });

        System.out.println("Download complete.");
    }

    /**
     * Creates a StreamingLocator for the specified asset and with the specified streaming policy name.
     * Once the StreamingLocator is created the output asset is available to clients for playback.
     *
     * @param manager       The entry point of Azure Media resource management
     * @param resourceGroup The name of the resource group within the Azure subscription
     * @param accountName   The Media Services account name
     * @param assetName     The name of the output asset
     * @param locatorName   The StreamingLocator name (unique in this case)
     * @return The locator created
     */
    private static StreamingLocator getStreamingLocator(MediaServicesManager manager, String resourceGroup, String accountName,
                                                        String assetName, String locatorName) {

        // Observe que estamos usando um dos PredefinedStreamingPolicies que informam o componente Origin
        // dos Serviços de Mídia do Azure como publicar o conteúdo para streaming.
        System.out.println("Creating a streaming locator...");
        StreamingLocator locator = manager
                .streamingLocators().define(locatorName)
                .withExistingMediaService(resourceGroup, accountName)
                .withAssetName(assetName)
                .withStreamingPolicyName("Predefined_ClearStreamingOnly")
                .create();

        return locator;
    }

    /**
     * Checks if the streaming endpoint is in the running state, if not, starts it.
     *
     * @param manager           The entry point of Azure Media resource management
     * @param resourceGroup     The name of the resource group within the Azure subscription
     * @param accountName       The Media Services account name
     * @param locatorName       The name of the StreamingLocator that was created
     * @param streamingEndpoint The streaming endpoint.
     * @return List of streaming urls
     */
    private static List<String> getStreamingUrls(MediaServicesManager manager, String resourceGroup, String accountName,
                                                 String locatorName, StreamingEndpoint streamingEndpoint) {
        List<String> streamingUrls = new ArrayList<>();

        ListPathsResponse paths = manager.streamingLocators().listPaths(resourceGroup, accountName, locatorName);

        for (StreamingPath path : paths.streamingPaths()) {
            StringBuilder uriBuilder = new StringBuilder();
            uriBuilder.append("https://")
                    .append(streamingEndpoint.hostname())
                    .append("/")
                    .append(path.paths().get(0));

            streamingUrls.add(uriBuilder.toString());
        }
        return streamingUrls;
    }

    /**
     * Cleanup
     *
     * @param manager               The entry point of Azure Media resource management.
     * @param resourceGroupName     The name of the resource group within the Azure subscription.
     * @param accountName           The Media Services account name.
     * @param transformName         The transform name.
     * @param jobName               The job name.
     * @param assetName             The asset name.
     * @param streamingLocatorName  The streaming locator name.
     * @param stopEndpoint          Stop endpoint if true, otherwise keep endpoint running.
     * @param streamingEndpointName The endpoint name.
     */
    private static void cleanup(MediaServicesManager manager, String resourceGroupName, String accountName, String transformName, String jobName,
                                String assetName, String streamingLocatorName, boolean stopEndpoint, String streamingEndpointName) {
        if (manager == null) {
            return;
        }

        manager.jobs().delete(resourceGroupName, accountName, transformName, jobName);
        manager.assets().delete(resourceGroupName, accountName, assetName);

        manager.streamingLocators().delete(resourceGroupName, accountName, streamingLocatorName);

        if (stopEndpoint) {

        // Como iniciamos o endpoint, vamos interrompê-lo.
            manager.streamingEndpoints().stop(resourceGroupName, accountName, streamingEndpointName);
        } else {

        // Manteremos o endpoint em execução porque ele não foi iniciado por este exemplo. Observe que há custos para mantê-lo funcionando.
        // Consulte https://azure.microsoft.com/en-us/pricing/details/media-services/ para obter preços.
            System.out.println("The endpoint '" + streamingEndpointName + "' is running. To halt further billing on the endpoint, please stop it in azure portal or AMS Explorer.");
        }
    }
}

