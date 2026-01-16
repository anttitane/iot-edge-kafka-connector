# Stage 1: Build
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy project file(s) and restore as a distinct layer
COPY ["iot-edge-kafka-connector.csproj", "./"]
RUN dotnet restore "iot-edge-kafka-connector.csproj"

# Copy the remaining source and build
COPY . .
RUN dotnet build "iot-edge-kafka-connector.csproj" -c Release -o /app/build

# Stage 2: Publish
FROM build AS publish
RUN dotnet publish "iot-edge-kafka-connector.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Stage 3: Runtime
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app

# Run as non-root user (numeric UID)
ARG APP_UID=1000
RUN chown -R "$APP_UID":"$APP_UID" /app
USER $APP_UID

COPY --from=publish /app/publish .

ENTRYPOINT ["dotnet", "iot-edge-kafka-connector.dll"]
