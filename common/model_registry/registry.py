from common.model_registry.tracking import connect_tracking_server, fetch_run_id

CHAMPION_ALIAS = "champion"


def fetch_champion_version(model_name):
    client = connect_tracking_server()
    models = client.search_registered_models(filter_string=f"name = '{model_name}'")
    if not models or CHAMPION_ALIAS not in models[0].aliases:
        return None
    version = models[0].aliases[CHAMPION_ALIAS]
    return version, client.get_model_version(model_name, version).run_id


def compare_metrics(metric, candidate_value, champion_value, lower_is_better):
    better = candidate_value < champion_value if lower_is_better else candidate_value > champion_value
    if better:
        return []
    return [f"{metric} {candidate_value:.4g} not better than champion {champion_value:.4g}"]


def register_model(client, model_name, run_id):
    if not client.search_registered_models(filter_string=f"name = '{model_name}'"):
        client.create_registered_model(model_name)
    return client.create_model_version(model_name, source=f"runs:/{run_id}", run_id=run_id).version


def validate_candidate_model(spec, run_name, champion_version, champion_failed):
    # 학습 run(run_name)을 모델 버전으로 등록하고, 같은 시험 데이터로 평가한 champion보다 나으면 champion으로 승격
    model_name = spec["name"]
    metric = spec["evaluation"]["metric"]
    client = connect_tracking_server()
    _, train_run_id = fetch_run_id(client, model_name, run_name)
    _, eval_run_id = fetch_run_id(client, model_name, f"{run_name}_eval")

    failures = []
    if champion_failed:
        failures.append("champion evaluation failed")
    elif champion_version is not None:
        _, champion_eval_run_id = fetch_run_id(client, model_name, f"{run_name}_champion_eval")
        failures = compare_metrics(
            metric,
            client.get_run(eval_run_id).data.metrics[metric],
            client.get_run(champion_eval_run_id).data.metrics[metric],
            spec["evaluation"]["lower_is_better"],
        )
    promoted = not failures

    version = register_model(client, model_name, train_run_id)
    client.set_model_version_tag(model_name, version, "validation.decision", "promoted" if promoted else "rejected")
    client.set_model_version_tag(model_name, version, "validation.reason", "; ".join(failures) or "passed all checks")
    client.set_model_version_tag(model_name, version, "validation.compared_champion_version", champion_version or "none")
    client.set_model_version_tag(model_name, version, "validation.eval_run_id", eval_run_id)
    client.set_model_version_tag(model_name, version, "validation.metric", metric)
    client.set_tag(train_run_id, "aiems.model_version", version)
    client.set_tag(train_run_id, "aiems.promoted", str(promoted).lower())
    if promoted:
        client.set_registered_model_alias(model_name, CHAMPION_ALIAS, version)
    return {"version": version, "promoted": promoted, "failures": failures}
