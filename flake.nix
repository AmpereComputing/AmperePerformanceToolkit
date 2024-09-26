{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages."${system}".extend (
          pkgs: prev: with pkgs; {
            pythonPackagesExtensions = prev.pythonPackagesExtensions ++ [
              (
                pythonFinal: pythonPrev: with pythonFinal; {
                  protobuf = protobuf5;

                  click803 = click.overrideAttrs (
                    f: p: {
                      version = "8.0.3";

                      src = fetchPypi {
                        inherit (f) pname version;
                        sha256 = "sha256-QQ6TKwUPXu13PEzalN51lxyJzbMVWnKggxE5p55ey1s=";
                      };

                      doInstallCheck = false;
                    }
                  );

                  proto-plus = pythonPrev.proto-plus.overrideAttrs (
                    f: p: {
                      version = "1.24.0";

                      src = fetchFromGitHub {
                        owner = "googleapis";
                        repo = "proto-plus-python";
                        rev = "v${f.version}";
                        hash = "sha256-pTbraH2l9AH2sODi3Zd1A2IBkiU8aHVVTSa/h7i0m28=";
                      };
                    }
                  );

                  grpc-google-iam-v1 = pythonPrev.grpc-google-iam-v1.overrideAttrs (
                    f: p: {
                      version = "0.13.1";

                      src = fetchFromGitHub {
                        owner = "googleapis";
                        repo = "python-grpc-google-iam-v1";
                        rev = "v${f.version}";
                        hash = "sha256-FLDx2zbM0qqLa+k/7xexyv5/YHlSOdikrbU2eYbxDM0=";
                      };
                    }
                  );

                  google-api-core = pythonPrev.google-api-core.overrideAttrs (
                    f: p: {
                      version = "2.20.0";

                      src = fetchFromGitHub {
                        owner = "googleapis";
                        repo = "python-api-core";
                        rev = "v${f.version}";
                        hash = "sha256-ccjkGQNaPRefI6+j/O+NwdBGEVNuZ5q5m1d8EAJGcbs=";
                      };
                    }
                  );

                  google-cloud-kms = pythonPrev.google-cloud-kms.overrideAttrs (
                    f: p: {
                      version = "3.0.0";

                      src = fetchFromGitHub {
                        owner = "googleapis";
                        repo = "google-cloud-python";
                        rev = "google-cloud-kms-v${f.version}";
                        hash = "sha256-R6anDMiK5nrXapWYyAS9IaQAhmfA5bO8LEliftWKjAw=";
                      };

                      sourceRoot = "source/packages/google-cloud-kms";
                    }
                  );

                  google-cloud-bigquery = pythonPrev.google-cloud-bigquery.overrideAttrs (
                    f: p: { doInstallCheck = false; }
                  );

                  google-auth-oauthlib = pythonPrev.google-auth-oauthlib.override { click = click803; };

                  google-cloud-testutils = pythonPrev.google-cloud-testutils.override { click = click803; };
                }
              )
            ];
          }
        );
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            (python312.withPackages (
              pythonPackages: with pythonPackages; [
                absl-py
                beautifulsoup4
                blinker
                contextlib2
                colorama
                click803
                (colorlog.overrideAttrs (
                  f: p: {
                    version = "2.6.0";

                    src = fetchPypi {
                      inherit (f) pname version;
                      sha256 = "sha256-DwOuASihrC4i7GpmF++9NqsA1LLhxJxJfhGFTPJPH+k=";
                    };

                    doInstallCheck = false;
                  }
                ))
                (buildPythonPackage rec {
                  pname = "google-cloud-aiplatform";
                  version = "1.68.0";
                  format = "setuptools";

                  src = fetchPypi {
                    inherit pname version;
                    sha256 = "sha256-106fM3B8ehTGoyp8/prNMrkJdd+6n6xIfRBci6UZf0A=";
                  };

                  propagatedBuildInputs = [
                    docstring-parser
                    google-cloud-bigquery
                    google-cloud-resource-manager
                    google-cloud-storage
                    pydantic
                    shapely
                  ];
                })
                google-cloud-core
                google-cloud-datastore
                google-cloud-monitoring
                jinja2
                loguru
                numpy
                pandas
                pint
                ptyprocess
                (python-dotenv.override { click = click803; })
                pytz
                pywinrm
                pyyaml
                seaborn
                setuptools
                six
                tabulate
                timeout-decorator
              ]
            ))
            google-cloud-sdk
            openssl
            stdenv.cc.cc.lib
          ];
        };
      }
    );
}
