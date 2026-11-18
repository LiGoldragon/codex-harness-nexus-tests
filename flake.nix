{
  description = "Codex Harness Nexus behavioral tests.";
  inputs.base.url = "github:LiGoldragon/build-base";
  outputs = { self, base }: base.lib.workspaceOutputs {
    cargoNix = ./Cargo.nix;
    extraChecks = { pkgs, toolchain, ... }: {
      fmt = pkgs.runCommand "codex-harness-nexus-tests-fmt" { nativeBuildInputs = [ toolchain ]; } ''
        rustfmt --edition 2024 --check ${./src}/lib.rs ${./tests}/transitions.rs ${./tests}/nexus.rs
        touch "$out"
      '';
    };
  };
}
